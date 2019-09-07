package pers.nebo.hdfs.parseandfilter;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * 解析机顶盒用户原始数据
 */
public class ParseAndFilterLog extends Configured implements Tool {
	
	/*
	 * 只需Mapper完成原始数据解析
	 */
	public static class ExtractTVMsgLogMapper extends
			//Mapper<LongWritable, BytesWritable, Text, Text> {
		Mapper<LongWritable, Text, Text, Text> {
		//public void map(LongWritable key, BytesWritable value, Context context)
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// 原始数据
			//String data = new String(value.getBytes(), 0, value.getLength());
			String data = value.toString();
			// 调用接口直接解析出我们需要数据格式
			// stbNum + "@" + date + "@" + sn + "@" + p+ "@" + s + "@" + e + "@"
			// + duration
			DataUtil.transData(data, context);
		}

	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: ParseAndFilterLog [<in>...] <out>");
			System.exit(2);
		}		

		Job job = Job.getInstance();
		
		// 设置输出key value分隔符
		job.getConfiguration().set("mapreduce.output.textoutputformat.separator", "@");

		job.setJarByClass(ParseAndFilterLog.class);
		job.setMapperClass(ExtractTVMsgLogMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//job.setInputFormatClass(SequenceFileInputFormat.class);
		// 设置输入路径
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}

		// 设置输出路径
		FileOutputFormat.setOutputPath(job, new Path(
				otherArgs[otherArgs.length - 1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}
	public static void main(String[] args) throws Exception {
    	int ec = ToolRunner.run(new Configuration(),new ParseAndFilterLog(), args);
    	System.exit(ec);
	}
}
