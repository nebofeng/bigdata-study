package com.nebo.parseandfilter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * 针对上一步的结果统计频道每天每分钟的收视指标
 * 
 */
public class AnalyzeCountChannelRating extends Configured implements Tool {
	public static class AnalyzeCountChannelRatingMapper extends
			Mapper<Object, Text, Text, Text> {
		// 存储当前在播数集合
		private Map<String, String> curNumMap = new HashMap<String, String>();

		/**
		 * 获取分布式缓存文件
		 */
		@SuppressWarnings("deprecation")
		protected void setup(Context context) throws IOException,
				InterruptedException {
			BufferedReader br;
			String infoAddr = null;
			// 返回缓存文件路径
			Path[] cacheFilesPaths = context.getLocalCacheFiles();
			for (Path path : cacheFilesPaths) {
				String pathStr = path.toString();
				br = new BufferedReader(new FileReader(pathStr));
				while (null != (infoAddr = br.readLine())) {
					// 按行读取并解析当前在播数据
					String[] tvjoin = StringUtils.split(infoAddr.toString(),
							"@");
					if (tvjoin.length == 3) {
						curNumMap.put(
								tvjoin[0].trim() + "@" + tvjoin[1].trim(),
								tvjoin[2].trim());
					}
				}
			}

		}

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// channel0 + "@" + date1 + "@" + min2+avgnum3 +reachnum4
			String[] kv = StringUtils.split(value.toString(), "@");
			if (kv.length != 5) {
				return;
			}
			// 平均收视人数
			int avgnum = Integer.parseInt(kv[3].trim());
			// 到达人数
			int reachnum = Integer.parseInt(kv[4].trim());
			// 当前在播数
			int currentStbnum = Integer.parseInt(curNumMap.get(kv[1].trim()
					+ "@" + kv[2].trim()));
			// 收视率
			float tvrating = (float) avgnum / 25000 * 100;
			// 市场份额
			float marketshare = (float) avgnum / currentStbnum * 100;
			// 到达率
			float reachrating = (float) reachnum / 25000 * 100;
			// 将计算的所有指标输出
			context.write(value, new Text(tvrating + "@" + reachrating + "@"
					+ marketshare));

		}
	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err
					.println("Usage: AnalyzeCountChannelRating cache in [<in>...] <out>");
			System.exit(2);
		}

		Job job = Job.getInstance();

		// 设置输出key value分隔符
		job.getConfiguration().set(
				"mapreduce.output.textoutputformat.separator", "@");
		// 添加缓存文件
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] dirstatus = fs.listStatus(new Path(otherArgs[0]));
		for (FileStatus file : dirstatus) {
			job.addCacheFile(file.getPath().toUri());
		}
		job.setJarByClass(AnalyzeCountChannelRating.class);
		job.setMapperClass(AnalyzeCountChannelRatingMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// 设置输入路径
		for (int i = 1; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}

		// 设置输出路径
		FileOutputFormat.setOutputPath(job, new Path(
				otherArgs[otherArgs.length - 1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int ec = ToolRunner.run(new Configuration(),
				new AnalyzeCountChannelRating(), args);
		System.exit(ec);
	}
}
