package pers.nebo.wcdemo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ author fnb
 * @ email nebofeng@gmail.com
 * @ date  2019/11/13
 * @ des :
 */
public class WCMapper extends Mapper<LongWritable,Text,Text,IntWritable>{

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] splits = value.toString().split(" ");
//		new StringTokenizer(value.toString()," ");
        for (String string : splits) {
            context.write(new Text(string), new IntWritable(1));
        }
    }
}
