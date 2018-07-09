package main.mapmultiinputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class ScoreCount extends Configured implements Tool{
	public static class ScoreMapper extends Mapper< Text, ScoreWritable, Text, ScoreWritable>{
		 @Override
		protected void map(Text key, ScoreWritable value, Context context)
				throws IOException, InterruptedException {
			 context.write(key, value);
		}
		
	 
	 
	}
	
	public static class ScoreReducer extends Reducer<Text, ScoreWritable, Text, Text>{
		 private Text text = new Text();
		@Override
		protected void reduce(Text key, Iterable<ScoreWritable> values,  Context context) throws IOException, InterruptedException {
			float totalScore=0.0f;
            float averageScore = 0.0f;
            for(ScoreWritable ss:values){
                totalScore +=ss.getChinese()+ss.getMath()+ss.getEnglish()+ss.getPhysics()+ss.getChemistry();
                averageScore +=totalScore/5;
            }
            text.set(totalScore+"\t"+averageScore);
            context.write(key, text);	 
		}
		
	}

	 

	public int run(String[] args) throws Exception {
		 Configuration conf = new Configuration();
		 Path myPath = new Path(args[1]);
		 FileSystem hdfs = myPath.getFileSystem(conf);
		 if(hdfs.isDirectory(myPath)){
			 hdfs.delete(myPath, true);//TODO:
		 }
		 
		 Job job =new Job(conf,"ScoreCount");
		 job.setJarByClass(ScoreCount.class);
		 FileInputFormat.addInputPath(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 
		 job.setMapperClass(ScoreMapper.class);
		 job.setReducerClass(ScoreReducer.class);
		 
		 job.setMapOutputKeyClass(Text.class);
		 job.setOutputValueClass(ScoreWritable.class);
		 
		 job.setInputFormatClass(ScoreInputFormat.class);
		 job.waitForCompletion(true);
		 
		 return 0;
	}
    
    public static void main(String[] args) throws Exception {
		String[] args0 = {
				"hdfs://nebo:9000/junior/score.txt",
                "hdfs://nebo:9000/junior/score-out/" 
		};
		
		int ec=ToolRunner.run(new Configuration(), new ScoreCount(),args0);
		System.exit(ec);
	}
}
 