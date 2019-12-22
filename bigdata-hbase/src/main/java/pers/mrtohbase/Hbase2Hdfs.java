package pers.mrtohbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
/**
 * https://blog.csdn.net/m0_37739193/article/details/76053636
 */

/**
 * @ author fnb
 * @ email nebofeng@gmail.com
 * @ date  2019/12/18
 * @ des :
 */
public class Hbase2Hdfs {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        String tablename = "hello";
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "h71");
        Job job = new Job(conf, "WordCountHbaseReader");
        job.setJarByClass(Hbase2Hdfs.class);
        Scan scan = new Scan();
        TableMapReduceUtil.initTableMapperJob(tablename,scan,doMapper.class, Text.class, Text.class, job);
        job.setReducerClass(WordCountHbaseReaderReduce.class);
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        MultipleOutputs.addNamedOutput(job, "hdfs", TextOutputFormat.class, WritableComparable.class, Writable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }



    public static class doMapper extends TableMapper<Text, Text> {

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            String rowValue = Bytes.toString(value.listCells().get(0).getValueArray());
            context.write(new Text(rowValue), new Text("one"));

        }

    }


    public static class WordCountHbaseReaderReduce extends Reducer<Text,Text,Text, NullWritable> {
        private Text result = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
            for(Text val:values){
                result.set(val);
                context.write(key, NullWritable.get());

            }

        }

    }

}