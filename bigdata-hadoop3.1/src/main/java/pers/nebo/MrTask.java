package pers.nebo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class MrTask {

    static class MrThread extends Thread {

        public  String inputPath;
        public String outputPath;
        String jobName;
        MrThread(String inputPath ,String  outputPath,String jobName ){
            this.inputPath=inputPath;
            this.outputPath=outputPath;
            this.jobName=jobName;
        }

        @Override
        public void run() {
            Configuration conf = new Configuration();
            Job job = null;
            try {
                job = Job.getInstance(conf, jobName);
            } catch (IOException e) {
                e.printStackTrace();
            }
            job.setJarByClass(MrTask.class);
            job.setMapperClass(TokenizerMapper.class);
            job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            try {
                //输入文件路径
                FileInputFormat.addInputPath(job, new Path(inputPath));
                // 输出文件路径
                FileOutputFormat.setOutputPath(job, new Path(outputPath));
                System.exit(job.waitForCompletion(true) ? 0 : 1);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length!=2){
            System.out.println("输入参数不符合规范 ！ ");
        }
        String inputPath =  args[0] ;
        String outputPath = args[1];

        MrThread mrThread1 = new MrThread("","","");
        MrThread mrThread2 = new MrThread("","","");
        mrThread1.start();
        mrThread2.start();

    }

}
