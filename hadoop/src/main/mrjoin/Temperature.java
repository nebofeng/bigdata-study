import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * @author NeboFeng ==》统计美国气象台30年来的平均气温 1.编写map（）函数 2.编写reduce()函数
 *         3.编写run()执行方法，负责执行mapreduce作业 4.在main（）方法中运行程序
 *
 */
public class Temperature extends Configured implements Tool{
	public static class TemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// 第一步，我们将每行气象站数据转换为String类型
			String line = value.toString();
			// 第二步 提取气温值
			int temperature = Integer.parseInt(line.substring(14, 19).trim());
			// 第三步 获取气象站编号

			// 过滤无效数据
			if (temperature != -9999) {
				// 获取输入分片
				FileSplit fileSplit = (FileSplit) context.getInputSplit();
				// 获取气象台编号
				String weatherStationId = "03103";

				//String weatherStationId = fileSplit.getPath().getName().substring(5, 10);
				// 第四步： 输出数据
				context.write(new Text(weatherStationId), new IntWritable(temperature));

			}
		}
	}
	
	 public static class TemperatureReducer extends Reducer<Text, IntWritable, Text, FloatWritable>{
		 private FloatWritable result = new FloatWritable();
		 public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException ,InterruptedException{
			//第一步： 统计相同气象站的所有气温值
			 int sum = 0 ;
			 int count = 0;
			 for(IntWritable val:values){
				 //对所有气温值累加
				 sum+=val.get();
				 //统计集合大小
				 count++;
			 }
			 //第二部 求同一个气象站的气温平均值
			 float answer = sum/count;
			 result.set(answer);
			 //第三部 输出数据 
			 context.write(key, result);
					 
		 }
		 
	 }

	public static void main(String[] args) throws Exception {
		// 数据输入路径 和输出路径
		String[] args0 = {
				"hdfs://nebo:9000/middle/weather/",
				"hdfs://nebo:9000/middle/weather/out3/"
		};
		int ec = ToolRunner.run(new Configuration(), new Temperature(), args0);
		System.exit(ec);
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//第一步  读取配置文件
		Configuration conf = new Configuration()  ;
		
		
		Path myPath =  new Path(args[1]);
		
		FileSystem hdfs = myPath.getFileSystem(conf);
		
		if(hdfs.isDirectory(myPath)){
			hdfs.delete(myPath, true);
		}
		
		Job job =new Job(conf, "temperature");//新建一个任务
		
		job.setJarByClass(Temperature.class);//设置主类
		
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(TemperatureMapper.class);
		job.setReducerClass(TemperatureReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		
		return job.waitForCompletion(true)?0:1;
	}

}

