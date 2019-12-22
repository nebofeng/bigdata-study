package pers.wcdemo;


import javafx.scene.text.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * @ author fnb
 * @ email nebofeng@gmail.com
 * @ date  2019/11/13
 * @ des :
 */
public class WCReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> iter,
                          Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable intWritable : iter) {
            sum+=intWritable.get();
        }
        Put put = new Put(key.toString().getBytes());
        put.addColumn("cf".getBytes(), "cf".getBytes(), String.valueOf(sum).getBytes());
        context.write(null, put);
    }
}
