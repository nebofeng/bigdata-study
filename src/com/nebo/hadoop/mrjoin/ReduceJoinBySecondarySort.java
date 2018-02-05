package com.nebo.hadoop.mrjoin;

import java.io.IOException;

import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReduceJoinBySecondarySort extends Configured implements Tool {

	
	//分区函数
	public static class KeyPartitioner  extends Partitioner< TextPair,Text>{
		public int getPartition(TextPair key,Text value,int numPartitions){
	        return (key.getFirst().hashCode()&Integer.MAX_VALUE)% numPartitions;
	    }
	}
	
	//分组函数
	public static  class GroupingComparator extends WritableComparator{
		 protected GroupingComparator(){
	         super(TextPair.class, true);
	     }
	     @Override
	     //Compare two WritableComparables.
	     public int compare(WritableComparable w1, WritableComparable w2){
	         TextPair ip1 = (TextPair) w1;
	         TextPair ip2 = (TextPair) w2;
	         Text l = ip1.getFirst();
	         Text r = ip2.getFirst();
	         return l.compareTo(r);
	     }
	}
	
	
	public static class JoinReducer extends Reducer< TextPair,Text,Text,Text>{

	    protected void reduce(TextPair key, Iterable< Text> values,Context context) throws IOException,InterruptedException{
	       System.out.println("reduce");
	    	Iterator< Text> iter = values.iterator();
	        Text stationName = new Text(iter.next());//气象站名称
	        while(iter.hasNext()){
	            Text record = iter.next();//天气记录的每条数据
	            Text outValue = new Text(stationName.toString()+"\\s"+record.toString());
	            context.write(key.getFirst(),outValue);
	        }
	    }        
	}
	
	
	public static class JoinRecordMapper extends Mapper< LongWritable,Text,TextPair,Text>{
	    
	    protected void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
	    	String[] arr = value.toString().split("\\s+");
//	    	StringUtils.split(value.toString(),"\\s+");//解析天气记录数据
	    	if(arr.length==3){
	    		//key=气象站id  value=天气记录数据
	    		context.write(new TextPair(arr[0],"1"),new Text(arr[1]+"\\s"+arr[2]));
	    	}  
	    }
	}
	
	
	public static class JoinStationMapper extends Mapper< LongWritable,Text,TextPair,Text>{
	    protected void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
	    	String[] arr = value.toString().split("\\s+");
//	        String[] arr = StringUtils.split(value.toString(),"\\s+");//解析气象站数据
	    	if(arr.length==2){//满足这种数据格式
	    		//key=气象站id  value=气象站名称
	            context.write(new TextPair(arr[0],"0"),new Text(arr[1]));
	        }
	    }
	}
	@Override
	public int run(String[] args) throws Exception {
		  Configuration conf = new Configuration();// 读取配置文件
	        
	        Path mypath = new Path(args[2]);
			FileSystem hdfs = mypath.getFileSystem(conf);// 创建输出路径
			if (hdfs.isDirectory(mypath)) {
				hdfs.delete(mypath, true);
			}
	        Job job = new Job(conf, "join");// 新建一个任务
	        job.setJarByClass(ReduceJoinBySecondarySort.class);// 主类
	        
	        Path recordInputPath = new Path(args[0]);//天气记录数据源
	        Path stationInputPath = new Path(args[1]);//气象站数据源
	        Path outputPath = new Path(args[2]);//输出路径
	        
	        MultipleInputs.addInputPath(job,recordInputPath,TextInputFormat.class,JoinRecordMapper.class);//读取天气记录Mapper
	        MultipleInputs.addInputPath(job,stationInputPath,TextInputFormat.class,JoinStationMapper.class);//读取气象站Mapper
	        
	        FileOutputFormat.setOutputPath(job,outputPath);
	        job.setReducerClass(JoinReducer.class);// Reducer
	        
	        job.setPartitionerClass(KeyPartitioner.class);//自定义分区
	        job.setGroupingComparatorClass(GroupingComparator.class);//自定义分组
	        
	        job.setMapOutputKeyClass(TextPair.class);
	        job.setMapOutputValueClass(Text.class);
	        
	        
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
	        
 	      
	        
	        return job.waitForCompletion(true)?0:1;
	        
	        
	        
	}
	
	public static void main(String[] args) throws Exception{
    	String[] args0 = {"hdfs://nebo:9000/mrredjoin/records.txt"
    			,"hdfs://nebo:9000/mrredjoin/station.txt"
    			,"hdfs://nebo:9000/mrredjoin/join-out"
    	};
        int exitCode = ToolRunner.run(new ReduceJoinBySecondarySort(),args0);
        System.exit(exitCode);
}
	

}
