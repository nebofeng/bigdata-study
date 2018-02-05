package com.nebo.hadoop;

import java.io.IOException;


import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
 import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

 

public class Anagram extends Configured  implements Tool {
	
	public static class AnagramMapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value,  Context context)
				throws IOException, InterruptedException {
			 //第一步首先将获取的word 排序
			String word = value.toString();
			String sortWord = Sort(word);			
			context.write(new Text(sortWord), new Text(word));
	 		
		}
		
		public String Sort(String word){
			char[] sortWord = word.trim().toCharArray();
			Arrays.sort(sortWord);
			return String.valueOf(sortWord) ;
			//toString  [C@15a8cf03  ?? todo:
		}
	}
	
	
	public static class AnagramReducer extends Reducer<Text, Text, Text, Text>{
		
		@Override
		protected void reduce(Text text1, Iterable<Text> values,  Context context)
				throws IOException, InterruptedException {
			 //唯一的key
			 StringBuffer result = new StringBuffer() ;
			 boolean first = true ;
			 for(Text val:values){
				 //对所有的唯一key 对应的values 拼接。
				 if(first == true){
			      result =  new StringBuffer(val.toString())   ; 
			      first =  false;
				 }else{
					 
				  result = result.append(","+val)	; 
				 }
				 
				 
			 };
			  context.write(text1, new Text(result.toString()));
			 
		}
 	}
	
	 	
	 public static void main(String[] args) throws Exception {
		// 数据输入路径 和输出路径
				String[] args0 = {
						"hdfs://nebo:9000/anagram/",
						"hdfs://nebo:9000/anagram/out/"
				};
				int ec = ToolRunner.run(new Configuration(), new Anagram(), args0);
				System.exit(ec);
	}

	
	@Override
	public int run(String[] args) throws Exception {
		 
				//第一步  读取配置文件
				Configuration conf = new Configuration()  ;
				
				
				Path myPath =  new Path(args[1]);
				
				FileSystem hdfs = myPath.getFileSystem(conf);
				
				if(hdfs.isDirectory(myPath)){
					hdfs.delete(myPath, true);
				}
				
				Job job =new Job(conf, "anagram");//新建一个任务
				
				job.setJarByClass(Anagram.class);//设置主类
				
				FileInputFormat.addInputPath(job,new Path(args[0]));
				FileOutputFormat.setOutputPath(job, new Path(args[1]));
				
				job.setMapperClass(AnagramMapper.class);
				job.setReducerClass(AnagramReducer.class);
				
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				
				
				
				return job.waitForCompletion(true)?0:1;
	}
	
	
	
	
	 
	
	

}
