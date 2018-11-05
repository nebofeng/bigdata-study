package com.nebo.disfiles;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TVCount extends Configured implements Tool {
	
	public static class TVMapper extends Mapper<Text, TVWritable, Text, TVWritable>{

		@Override
		protected void map(Text key, TVWritable value, Context context)
				throws IOException, InterruptedException {
			 
			super.map(key, value, context);
		}
		
	}

	public static class TVReducer extends Reducer<Text, TVWritable, Text, Text>{
		private MultipleOutputs<Text, Text> multipleOutputs;

		@Override
		protected void setup(Context context) throws IOException ,InterruptedException{
			multipleOutputs = new MultipleOutputs< Text, Text>(context);
		}
		protected void reduce(Text Key, Iterable< TVWritable> Values,Context context) throws IOException, InterruptedException {
			String names = Key.toString().trim();
			
			String type =names.substring(names.length()-1,names.length());
			String tvName = names.substring(0,names.length()-1).trim() ;
			String fileName = null;
			switch(type){
				case "1"://1优酷2搜狐3土豆4爱奇艺5迅雷看看
					fileName = "youku";
					break;					
				case "2":
					fileName = "souhu";
					break;
				case "3":
					fileName = "tudou";
					break;
				case "4":
					fileName = "aiqiyi";
					break;
				case "5":
					fileName = "xunlei";
					break;
			}
 			int a=0;
			int b=0;
			int c=0;
			int d =0;
			int e = 0;
			for(TVWritable value:Values){
				a+=value.getBfCount();
				b+=value.getScCount();
				c+=value.getPlCount();
				d+=value.getDzCount();
				e+=value.getcCount();
			}
			multipleOutputs.write(new Text(tvName), new Text(new TVWritable(a,b,c,d,e).toString()) ,  fileName);
		}
			
		@Override
		protected void cleanup(Context context) throws IOException ,InterruptedException{
			multipleOutputs.close();
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		 Configuration conf = new Configuration();
		 Path myPath = new Path(args[1]);
		 FileSystem hdfs = myPath.getFileSystem(conf);
		 if(hdfs.isDirectory(myPath)){
			 hdfs.delete(myPath, true);
		 }			 
		 Job job =new Job(conf,"TVCount");
		 job.setJarByClass(TVCount.class);
		 FileInputFormat.addInputPath(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 
		 job.setMapperClass(TVMapper.class);
		 job.setReducerClass(TVReducer.class);
		 
		 job.setMapOutputKeyClass(Text.class);
		 job.setOutputValueClass(TVWritable.class);

		 job.setInputFormatClass(TextInputFormat.class);
		 job.setInputFormatClass(TVPlayInputFormat.class);
		 job.waitForCompletion(true);		 
		 return 0;
 		
	}
	
	public static void main(String[] args) throws Exception {
		String[] args0 = {
				"hdfs://nebo:9000/tvplay/tvplay.txt",
                "hdfs://nebo:9000/tvplay/tvplay-out/" 
		};
		
		int ec=ToolRunner.run(new Configuration(), new TVCount(),args0);
		System.exit(ec);
	}

}
