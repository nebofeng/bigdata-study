package pers.nebo.hdfs.disfiles;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;


public class TVPlayInputFormat extends FileInputFormat <Text, TVWritable>{

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		 
		return false;
	}
	@Override
	public RecordReader<Text, TVWritable> createRecordReader(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		return new TVPlayRecordReader();
 		 
	}
	public static class TVPlayRecordReader extends RecordReader<Text ,TVWritable> {
		public LineReader in;//行读取器
        public Text lineKey;//自定义key类型
        public TVWritable lineValue;//自定义value类型
        public Text line;//每行数据类型
		@Override
		public void close() throws IOException {
			 if(in!=null){
				 in.close();
			 }
		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			return lineKey;
		}
		@Override
		public TVWritable getCurrentValue() throws IOException, InterruptedException {
		 
			return lineValue;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {

			return 0;
		}
		@Override
		public void initialize(InputSplit input, TaskAttemptContext context) throws IOException, InterruptedException {
			 FileSplit split =(FileSplit)input;
			 Configuration job = context.getConfiguration();
			 Path file=split.getPath();
			 FileSystem fs = file.getFileSystem(job);
			 FSDataInputStream fillin =fs.open(file);
			 in =new LineReader(fillin,job);
			 line=new Text();
			 lineKey=new Text();
			 lineValue = new TVWritable();
		}
		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			int lineSize = in.readLine(line);
			if(lineSize==0){
				return false;
			}
			String[] pireces = line.toString().split("\\s+");
			if(pireces.length < 7){
				throw new IOException("Invaild record received");	
			}
			//获取数据转换为int
			int a,b,c,d,e;
			try {
				a=Integer.parseInt(pireces[pireces.length-5].trim());
				b=Integer.parseInt(pireces[pireces.length-4].trim());
				c=Integer.parseInt(pireces[pireces.length-3].trim());
				d=Integer.parseInt(pireces[pireces.length-2].trim());
				e=Integer.parseInt(pireces[pireces.length-1].trim());
			} catch (NumberFormatException nfe) {
				throw new IOException("Error parsing floating poing value in record");
			}
			String result = "";
			for(int i =0;i<pireces.length-5;i++){
				result= result+pireces[i];
			}
			lineKey.set(result);
			//lineKey.set(pireces[0]+"\t"+pireces[1]);//完成自定义key
			lineValue.set(a, b, c, d, e);
			return true;
		}

		 
	 
	}
	
	

}
