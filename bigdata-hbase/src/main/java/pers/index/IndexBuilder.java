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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * @ author fnb
 * @ email nebofeng@gmail.com
 * @ date  2019/7/26
 * @ des :
 */
public class IndexBuilder {
    static  class Map extends  TableMapper <ImmutableBytesWritable, Put>{

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            super.map(key, value, context);
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }


    }

    public static void main(String[] args) throws IOException{
        Configuration conf= HBaseConfiguration.create();
        String[] otherArgs=new GenericOptionsParser(conf,args).getRemainingArgs();


        //IndexBuilder: TableName ,ColumnFamily,Qualifier ...
        if(otherArgs.length<3){
            System.exit(-1);
        }

        String tableName=otherArgs[0];
        String columFamily=otherArgs[1];
        conf.set("tableName",tableName);
        conf.set("columnFamily",columFamily);

        String[] quailfiers=new String[otherArgs.length-2];

        conf.setStrings("quailfiers",quailfiers);
        Job job=new Job(conf,tableName);
        job.setJarByClass(IndexBuilder.class);
        job.setMapperClass(Map.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(TableInputFormat.class);
        job.setOutputFormatClass(MultiTableOutputFormat.class);

        Scan scan= new Scan();
        scan.setCaching(100);

        TableMapReduceUtil.initTableMapperJob(tableName,scan,Map.class,ImmutableBytesWritable.class,Put.class,job);
        try {
            job.waitForCompletion(true);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


    }

}


