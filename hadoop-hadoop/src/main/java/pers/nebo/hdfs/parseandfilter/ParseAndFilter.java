package pers.nebo.hdfs.parseandfilter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @auther nebofeng
 * @email nebofeng@gmail.com
 * @date 2018/12/3
 * @des : 清洗过滤数据提取有效字段
 */
public class ParseAndFilter {
    public static class TVLogMapper  extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // 原始数据
            //String data = new String(value.getBytes(), 0, value.getLength());
            String data = value.toString();
            // 调用接口直接解析出我们需要数据格式
            // stbNum + "@" + date + "@" + sn + "@" + p+ "@" + s + "@" + e + "@"
            // + duration
            DataUtil.transData(data, context);
        }
    }

    public static class  TVLogReducer  extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Text value, Text output, Text reporter) throws IOException {

        }
    }

     public static void main(String[] args) {

    }


}

/**

public static class ExtractTVMsgLogMapper extends
        //Mapper<LongWritable, BytesWritable, Text, Text> {
        Mapper<LongWritable, Text, Text, Text> {
    //public void map(LongWritable key, BytesWritable value, Context context)
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // 原始数据
        //String data = new String(value.getBytes(), 0, value.getLength());
        String data = value.toString();
        // 调用接口直接解析出我们需要数据格式
        // stbNum + "@" + date + "@" + sn + "@" + p+ "@" + s + "@" + e + "@"
        // + duration
        DataUtil.transData(data, context);
    }

}

    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: ParseAndFilterLog [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance();

        // 设置输出key value分隔符
        job.getConfiguration().set("mapreduce.output.textoutputformat.separator", "@");

        job.setJarByClass(ParseAndFilterLog.class);
        job.setMapperClass(ExtractTVMsgLogMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //job.setInputFormatClass(SequenceFileInputFormat.class);
        // 设置输入路径
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        // 设置输出路径
        FileOutputFormat.setOutputPath(job, new Path(
                otherArgs[otherArgs.length - 1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }
    public static void main(String[] args) throws Exception {
        int ec = ToolRunner.run(new Configuration(),new ParseAndFilterLog(), args);
        System.exit(ec);


 */
