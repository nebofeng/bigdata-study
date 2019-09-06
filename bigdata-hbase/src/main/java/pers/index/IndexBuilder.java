package pers.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * @ author fnb
 * @ email nebofeng@gmail.com
 * @ date  2019/7/26
 * @ des :hbase 离线批量构建二级索引的示例代码
 */
public class IndexBuilder {

    static class MyMap extends TableMapper<ImmutableBytesWritable, Put> {
        private Map<byte[], ImmutableBytesWritable> indexs = new HashMap<byte[], ImmutableBytesWritable>();
        private String familyName;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            String tableName = conf.get("tableName");
            String familyName = conf.get("familyName");
            String[] qualifiters = conf.getStrings("qualifiters");
            for (String q : qualifiters) {
                indexs.put(Bytes.toBytes(q), new ImmutableBytesWritable(Bytes.toBytes(tableName + "-" + q)));
            }
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            super.map(key, value, context);
            Set<byte[]> keys = indexs.keySet();
            for (byte[] k : keys) {
                ImmutableBytesWritable indexTableName = indexs.get(k);

                //获取一行数据中的colf：col
                byte[] val = value.getValue(Bytes.toBytes(familyName), k);
                if (val != null) {
                    Put put = new Put(val);
                    put.add(Bytes.toBytes("f1"), Bytes.toBytes("id"), key.get());
                    context.write(indexTableName, put);
                }
            }

        }


    }

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();


        //IndexBuilder: TableName ,ColumnFamily,Qualifier ...
        if (otherArgs.length < 3) {
            System.exit(-1);
        }

        String tableName = otherArgs[0];
        String columFamily = otherArgs[1];
        conf.set("tableName", tableName);
        conf.set("columnFamily", columFamily);

        String[] quailfiers = new String[otherArgs.length - 2];

        conf.setStrings("quailfiers", quailfiers);
        Job job = new Job(conf, tableName);
        job.setJarByClass(IndexBuilder.class);
        job.setMapperClass(MyMap.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(TableInputFormat.class);
        job.setOutputFormatClass(MultiTableOutputFormat.class);

        Scan scan = new Scan();
        scan.setCaching(100);

        TableMapReduceUtil.initTableMapperJob(tableName, scan, MyMap.class, ImmutableBytesWritable.class, Put.class, job);
        try {
            job.waitForCompletion(true);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


    }

}


