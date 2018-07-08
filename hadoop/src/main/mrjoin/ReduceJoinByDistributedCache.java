import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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

public class ReduceJoinByDistributedCache extends Configured implements Tool{
	
	//直接输出大表数据records.txt 
    public static class ReduceJoinByDistributedCacheMapper extends
            Mapper< LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
        	String[] arr=value.toString().split("\\s+");
//            String[] arr = StringUtils.split(value.toString(),"\\s+");
            if (arr.length == 3) {
                context.write(new Text(arr[0]), value);
            }
        }
    }
    
  //在reduce 端通过缓存文件实现join操作
    public static class ReduceJoinByDistributedCacheReducer extends
            Reducer< Text, Text, Text, Text> {
    	//定义Hashtable存放缓存数据
        private Hashtable< String, String> table = new Hashtable< String, String>();
        /**
         * 获取分布式缓存文件
         */
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
					// 按行读取并解析气象站数据
					String[] records = infoAddr.toString().split("\\s+");
//					String[] records = StringUtils.split(infoAddr.toString(),
//							"\\s+");
					if (null != records)//key为stationID，value为stationName
						table.put(records[0], records[1]);
				}
			}
        }
        public void reduce(Text key, Iterable< Text> values, Context context)
                throws IOException, InterruptedException {
        	//天气记录根据stationId 获取stationName
            String stationName = table.get(key.toString());
            for (Text value : values) {
                context.write(new Text(stationName), value);
            }
        }
    }
	
    public int run(String[] args) throws Exception {
    	Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: cache <in> [<in>...] <out>");
			System.exit(2);
		}

		//输出路径
		Path mypath = new Path(otherArgs[otherArgs.length - 1]);
		FileSystem hdfs = mypath.getFileSystem(conf);// 创建输出路径
		if (hdfs.isDirectory(mypath)) {
			hdfs.delete(mypath, true);
		}
		Job job = Job.getInstance(conf, "ReduceJoinByDistributedCache");

		//添加缓存文件
		job.addCacheFile(new Path(otherArgs[0]).toUri());//station.txt
		job.setJarByClass(ReduceJoinByDistributedCache.class);
		job.setMapperClass(ReduceJoinByDistributedCacheMapper.class);
		job.setReducerClass(ReduceJoinByDistributedCacheReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//添加输入路径
		for (int i = 1; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		//添加输出路径
		FileOutputFormat.setOutputPath(job, new Path(
				otherArgs[otherArgs.length - 1]));
		return job.waitForCompletion(true) ? 0 : 1;
    }
    public static void main(String[] args) throws Exception {
    	int ec = ToolRunner.run(new Configuration(),new ReduceJoinByDistributedCache(), args);
    	System.exit(ec);
	}

	 

}
