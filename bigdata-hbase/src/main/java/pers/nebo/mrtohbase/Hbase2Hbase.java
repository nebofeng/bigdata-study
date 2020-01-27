package pers.nebo.mrtohbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.Iterator;

/**
 * @ author fnb
 * @ email nebofeng@gmail.com
 * @ date  2019/12/18
 * @ des :
 */
public class Hbase2Hbase {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        String hbaseTableName1 = "hello";
        String hbaseTableName2 = "mytb2";
        prepareTB2(hbaseTableName2);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Hbase2Hbase.class);
        job.setJobName("mrreadwritehbase");
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        TableMapReduceUtil.initTableMapperJob(hbaseTableName1, scan, doMapper.class, Text.class, IntWritable.class, job);
        TableMapReduceUtil.initTableReducerJob(hbaseTableName2, doReducer.class, job);
        System.exit(job.waitForCompletion(true) ? 1 : 0);

    }


    public static class doMapper extends TableMapper<Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        @Override

        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

            //String rowValue = Bytes.toString(value.list().get(0).getValue());
            String rowValue = Bytes.toString(value.listCells().get(0).getValueArray());

            context.write(new Text(rowValue), one);

        }

    }


    public static class doReducer extends TableReducer<Text, IntWritable, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println(key.toString());
            int sum = 0;
            Iterator<IntWritable> haha = values.iterator();
            while (haha.hasNext()) {
                sum += haha.next().get();
            }
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(Bytes.toBytes("mycolumnfamily"), Bytes.toBytes("count"), Bytes.toBytes(String.valueOf(sum)));
            context.write(NullWritable.get(), put);

        }

    }


    public static void prepareTB2(String hbaseTableName) throws IOException {



        HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(hbaseTableName));
        HColumnDescriptor columnDesc = new HColumnDescriptor("mycolumnfamily");
        tableDesc.addFamily(columnDesc);
        Configuration cfg = HBaseConfiguration.create();
        Connection conn= ConnectionFactory.createConnection(cfg);
        Admin admin = conn.getAdmin();
        if (admin.tableExists(TableName.valueOf(hbaseTableName))) {
            System.out.println("Table exists,trying drop and create!");
            admin.disableTable(TableName.valueOf(hbaseTableName));
            admin.deleteTable(TableName.valueOf(hbaseTableName));
            admin.createTable(tableDesc);
        } else {

            System.out.println("create table: " + hbaseTableName);

            admin.createTable(tableDesc);

        }

    }
}