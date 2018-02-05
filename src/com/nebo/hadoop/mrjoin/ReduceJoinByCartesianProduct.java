package com.nebo.hadoop.mrjoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
 
/**
 * Description ： reduce端join 一对多。多对多
 * 
 * @author NeboFeng
 *
 */
public class ReduceJoinByCartesianProduct {

	public static class ReduceJoinByCartesianProductMapper  extends Mapper<Object, Text, Text, Text>{
			private Text joinKey=new Text();
	        private Text combineValue=new Text();
		 @Override
	        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	            String pathName=((FileSplit)context.getInputSplit()).getPath().toString();
	            //如果数据来自于records，加一个records的标记
	            if(pathName.endsWith("records.txt")){
	            	String[] valueItems = value.toString().split("\\s+");
//	            	String[] valueItems = value.toString().split(value.toString(),"\\s+");
//	                String[] valueItems=StringUtils.split(value.toString(),"\\s+");
	                //过滤掉脏数据
	                if(valueItems.length!=3){
	                    return;
	                }
	                joinKey.set(valueItems[0]);
	                combineValue.set("records.txt" + valueItems[1] + "\t" + valueItems[2]);
	            }else if(pathName.endsWith("station.txt")){
	                //如果数据来自于station，加一个station的标记
	            	String[] valueItems = value.toString().split("\\s+");
	                //过滤掉脏数据
	                if(valueItems.length!=2){
	                    return;
	                }
	                joinKey.set(valueItems[0]);
	                combineValue.set("station.txt" + valueItems[1]);
	            }
	            context.write(joinKey,combineValue);
	        }
		
	}
	
	
	/*
	 * reduce 端做笛卡尔积
	 */
	 public static class ReduceJoinByCartesianProductReducer extends Reducer<Text,Text,Text,Text>{
	        private List<String> leftTable=new ArrayList<String>();
	        private List<String> rightTable=new ArrayList<String>();
	        private Text result=new Text();
	        @Override
	        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	            //一定要清空数据
	            leftTable.clear();
	            rightTable.clear();
	            //相同key的记录会分组到一起，我们需要把相同key下来自于不同表的数据分开，然后做笛卡尔积
	            for(Text value : values){
	            	String val=value.toString();
	                if(val.startsWith("station.txt")){
	                    leftTable.add(val.replaceFirst("station.txt",""));
	                }else if(val.startsWith("records.txt")){
	                    rightTable.add(val.replaceFirst("records.txt",""));
	                }
	            }
	            //笛卡尔积
	            for(String leftPart:leftTable){
	                for(String rightPart:rightTable){
	                    result.set(leftPart+"\t"+rightPart);
	                    context.write(key, result);
	                }
	            }
	        }
	    }
	 
	 public static void main(String[] args) throws Exception{
	        Configuration conf = new Configuration();
	        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	        if (otherArgs.length < 2) {
	            System.err.println("Usage: reducejoin <in> [<in>...] <out>");
	            System.exit(2);
	        }
	        
	        //输出路径
	        Path mypath = new Path(otherArgs[otherArgs.length - 1]);
			FileSystem hdfs = mypath.getFileSystem(conf);// 创建输出路径
			if (hdfs.isDirectory(mypath)) {
				hdfs.delete(mypath, true);
			}
	        Job job = Job.getInstance(conf, "ReduceJoinByCartesianProduct");
	        job.setJarByClass(ReduceJoinByCartesianProduct.class);
	        job.setMapperClass(ReduceJoinByCartesianProductMapper.class);
	        job.setReducerClass(ReduceJoinByCartesianProductReducer.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
	        //添加输入路径
	        for (int i = 0; i < otherArgs.length - 1; ++i) {
	            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
	        }
	        //添加输出路径
	        FileOutputFormat.setOutputPath(job,
	                new Path(otherArgs[otherArgs.length - 1]));
	        System.exit(job.waitForCompletion(true) ? 0 : 1);
	    }
	
}
