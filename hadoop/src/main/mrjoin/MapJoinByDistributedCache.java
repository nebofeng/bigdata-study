import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Hashtable;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * 通过分布式缓存实现 map join
 * 适用场景：一个小表，一个大表
 */
public class MapJoinByDistributedCache extends Configured implements Tool {
	/*
	 * 直接在map 端进行join合并
	 */
	public static class MapJoinMapper extends
    Mapper< LongWritable, Text, Text, Text> {
		private Hashtable< String, String> table = new Hashtable< String, String>();//定义Hashtable存放缓存数据
        /**
         * 获取分布式缓存文件
         */
        @SuppressWarnings("deprecation")
		protected void setup(Context context) throws IOException,
                InterruptedException {
            Path[] localPaths = (Path[]) context.getLocalCacheFiles();//返回本地文件路径
            if (localPaths.length == 0) {
                throw new FileNotFoundException(
                        "Distributed cache file not found.");
            }
            FileSystem fs = FileSystem.getLocal(context.getConfiguration());//获取本地 FileSystem 实例
            FSDataInputStream in = null;
            
            in = fs.open(new Path(localPaths[0].toString()));// 打开输入流
            BufferedReader br = new BufferedReader(new InputStreamReader(in));// 创建BufferedReader读取器
            String infoAddr = null;
            while (null != (infoAddr = br.readLine())) {// 按行读取并解析气象站数据
                String[] records = infoAddr.split("\t");
                table.put(records[0], records[1]);//key为stationID，value为stationName
            }
        }
        
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
        	 String[] valueItems = StringUtils.split(value.toString(),"\\s+");
        	 String stationName = table.get(valueItems[0]);//天气记录根据stationId 获取stationName
        	 if(null !=stationName)
        	 context.write(new Text(stationName), value);
        }
       
		
	}
	
	public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = new Configuration();

        Path out = new Path(args[2]);
        FileSystem hdfs = out.getFileSystem(conf);// 创建输出路径
        if (hdfs.isDirectory(out)) {
            hdfs.delete(out, true);
        }
        Job job = Job.getInstance();//获取一个job实例
        job.setJarByClass(MapJoinByDistributedCache.class);
        FileInputFormat.addInputPath(job,
                new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(
                args[2]));
        //添加分布式缓存文件 station.txt
        job.addCacheFile(new URI(args[1]));
        job.setMapperClass(MapJoinMapper.class);
        job.setOutputKeyClass(Text.class);// 输出key类型
        job.setOutputValueClass(Text.class);// 输出value类型
        return job.waitForCompletion(true)?0:1;
    }
    public static void main(String[] args) throws Exception {
    	String[] args0 = {"hdfs://djt002:9000/join/records.txt"
    			,"hdfs://djt002:9000/join/station.txt"
    			,"hdfs://djt002:9000/join/mapcache-out"
    	};
        int ec = ToolRunner.run(new Configuration(),
                new MapJoinByDistributedCache(), args);
        System.exit(ec);
    }
}
