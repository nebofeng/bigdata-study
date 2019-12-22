package pers.mrtohbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @ author fnb
 * @ email nebofeng@gmail.com
 * @ date  2019/12/21
 * @ des :
 */
public class Hbase2HbaseMap {
    /**
     * 需求1
     * <p>
     * map 阶段写入到 hbase中 mapTest 表中
     * （1） 写入一个表
     * context.write(NullWritable.get(), put);
     * job.setOutputFormatClass(TableOutputFormat.class);
     * <p>
     * （2） 写入多个表
     * context.write(indexName, put);
     * <p>
     * （1.1） job.setOutputFormatClass(MultiTableOutputFormat.class);
     * <p>
     * 设置reduce个数为 0 job.setNumReduceTasks(0);
     * <p>
     * reduce阶段写入到 hbase中  ReduceTEST
     * (1) 写入一个表
     * context.write(NullWritable.get(), put);
     * （1.1）job.setOutputFormatClass(TableOutputFormat.class);
     * <p>
     * （1.2）TableMapReduceUtil.initTableReducerJob(hbaseTableName2, doReducer.class, job);
     * <p>
     * <p>
     * <p>
     * （2） 写入多个表
     * context.write(indexTableName, put);
     * （1.1） job.setOutputFormatClass(MultiTableOutputFormat.class);
     * <p>
     * <p>
     * TableMapReduceUtil.initTableMapperJob  从哪个表获取数据
     * TableMapReduceUtil.initTableReducerJob 输出到哪个表
     */
    public static class HbaseMapper extends TableMapper<NullWritable, Put> {


        List<byte[]> qs = new ArrayList<byte[]>();
        byte[] fam = Bytes.toBytes("log");

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            qs.add(Bytes.toBytes("city"));
            qs.add(Bytes.toBytes("country"));
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result values, Context context) throws IOException, InterruptedException {


            System.out.println(Bytes.toString(key.get()));
            for (int i = 0; i < qs.size(); i++) {
                byte[] value = values.getValue(fam, qs.get(i));
                //key --- rowkey
                Put put = new Put(key.get());
                put.addColumn(fam, qs.get(i), value);
                context.write(NullWritable.get(), put);


            }


        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "node1,node2,node3");
        conf.set(TableOutputFormat.OUTPUT_TABLE, "maptohbase1");
        Job job = Job.getInstance(conf);

        job.setMapperClass(HbaseMapper.class);

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);

        job.setJarByClass(Hbase2HbaseMap.class);

        /**
         * 这里不何止为0的话，就会只有一行输出，应该是默认的reduce导致的。 代码有空再看
         */
        job.setNumReduceTasks(0);
        job.setInputFormatClass(TableInputFormat.class);
        job.setOutputFormatClass(TableOutputFormat.class);

        TableMapReduceUtil.initTableMapperJob("eventlog", scan, HbaseMapper.class, NullWritable.class, Put.class, job);

        System.exit(job.waitForCompletion(true) ? 1 : 0);


    }

}
