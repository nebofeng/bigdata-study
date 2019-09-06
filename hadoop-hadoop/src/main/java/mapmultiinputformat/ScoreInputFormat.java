package mapmultiinputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

 
public class ScoreInputFormat extends FileInputFormat<Text, ScoreWritable>{
	
	protected boolean isSplitable(FileSystem fs, Path filename) {
		 
		return false;
	}
	
 
	@Override
	public RecordReader<Text, ScoreWritable> createRecordReader(org.apache.hadoop.mapreduce.InputSplit arg0,
			TaskAttemptContext arg1) throws IOException, InterruptedException {
 		return new ScoreRecordReader();
	}
 
	public static class ScoreRecordReader extends RecordReader< Text, ScoreWritable >{
		
		public LineReader in;//行读取器
        public Text lineKey;//自定义key类型
        public ScoreWritable lineValue;//自定义value类型
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
		public ScoreWritable getCurrentValue() throws IOException, InterruptedException {
			 
			return lineValue;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			 
			return 0;
		}

		@Override
		public void initialize(InputSplit input, TaskAttemptContext context) throws IOException, InterruptedException {
			 FileSplit split = (FileSplit)input;
			 Configuration job = context.getConfiguration();
			 Path file=split.getPath();
			 FileSystem fs =file.getFileSystem(job);
			 FSDataInputStream filein=fs.open(file);
			 
			 in=new LineReader(filein, job);
			 line=new Text();
			 lineKey=new Text();
			 lineValue=new ScoreWritable();
			
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			int lineSzie=in.readLine(line);//每行数据
			if(lineSzie==0){
				return false;
			}
			String[] pieces =line.toString().split("\\s+");//解析每行书据
			if(pieces.length!=7){
				throw new IOException("Invaild record received");
			}
			//将学生的每门成绩转换为float类型
			float a,b,c,d,e;
			try {
				a=Float.parseFloat(pieces[2].trim());
				b=Float.parseFloat(pieces[3].trim());
				c=Float.parseFloat(pieces[4].trim());
				d=Float.parseFloat(pieces[5].trim());
				e=Float.parseFloat(pieces[6].trim());
			} catch (NumberFormatException nfe) {
				throw new IOException("Error parsing floating poing value in record");
			}
			lineKey.set(pieces[0]+"\t"+pieces[1]);//完成自定义key
			lineValue.set(a, b, c, d, e);
			return true;
		}
		 
		 
		 
	 }
}
