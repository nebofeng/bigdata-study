package pers.nebo.hdfs.parseandfilter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * 针对上一步的结果统计每个频道每天的收视人数和人均收视时长
 * 
 */
public class ExtractChannelNumAndTimelen   extends Configured implements Tool {
	public static class ExtractChannelNumAndTimelenMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// stbNum0+"@"+date1+"@"+sn2+"@"+p3+"@"+s4+"@"+e5+"@"+duration6
			String[] kv = StringUtils.split(value.toString(), "@");
			// 过滤掉不合格数据
			if (kv.length != 7) {
				return;
			}
			// 机顶盒号
			String stbnum = kv[0].trim();
			// 日期
			String date = kv[1].trim();
			// 节目
			String channel = kv[2].trim();

			String duration = kv[6].trim();
			// 输出每条记录用户的机顶盒号和时长
			context.write(new Text(channel + "@" + date), new Text(stbnum + "@"
					+ duration));
		}
	}

	public static class ExtractChannelNumAndTimelenReduce extends
			Reducer<Text, Text, Text, Text> {
		private Text result = new Text();
		// 定义收视人数集合
		private Set<String> set_num = new HashSet<String>();

		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			set_num.clear();
			int timelen = 0;
			for (Text value : values) {
				String[] arr = StringUtils.split(value.toString(), "@");
				set_num.add(arr[0]);
				// 满足到达条件
				if (arr.length > 1) {
					timelen += Integer.parseInt(arr[1]);
				}
			}
			int num = set_num.size();
			// 计算出每天的收视人数和人均收视时长
			result.set(num + "@" + timelen / num);
			context.write(key, result);
		}

	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err
					.println("Usage: ExtractChannelNumAndTimelen [<in>...] <out>");
			System.exit(2);
		}

		Job job = Job.getInstance();

		// 设置输出key value分隔符
		job.getConfiguration().set(
				"mapreduce.output.textoutputformat.separator", "@");

		job.setJarByClass(ExtractChannelNumAndTimelen.class);
		job.setMapperClass(ExtractChannelNumAndTimelenMapper.class);
		job.setReducerClass(ExtractChannelNumAndTimelenReduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

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
		int ec = ToolRunner.run(new Configuration(),
				new ExtractChannelNumAndTimelen(), args);
		System.exit(ec);
	}
}
