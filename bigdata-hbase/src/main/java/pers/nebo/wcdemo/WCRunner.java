package pers.nebo.wcdemo;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * @ author fnb
 * @ email nebofeng@gmail.com
 * @ date  2019/11/13
 * @ des :
 */
public class WCRunner {
    public static void main(String[] args) throws  Exception {
        // 配置文件设置
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "node1,node2,node3");
        conf.set("fs.defaultFS", "hdfs://node1:8020");

        Job job = Job.getInstance(conf);
        job.setJarByClass(WCRunner.class);

        job.setMapperClass(WCMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);


        TableMapReduceUtil.initTableReducerJob("wc", WCReducer.class, job, null, null, null, null, false);
        FileInputFormat.addInputPath(job, new Path("/usr/wc"));
        // reduce端输出的key和value的类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Put.class);

        // job.setOutputFormatClass(cls);
        // job.setInputFormatClass(cls);

        job.waitForCompletion(true);
    }
}
