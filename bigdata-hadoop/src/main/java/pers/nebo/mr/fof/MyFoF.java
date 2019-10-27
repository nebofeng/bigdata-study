package pers.nebo.mr.fof;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * @ author fnb
 * @ email nebofeng@gmail.com
 * @ date  2019/10/27
 * @ des :  共同好友推荐
 *
 */
public class MyFoF {

    static class FMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        Text mkey = new Text();
        IntWritable mval = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            //value:
            //tom       hello hadoop cat   :   hello:hello  1
            //hello     tom world hive mr      hello:hello  02
            String[] strs = StringUtils.split(value.toString(), ' ');

            String user = strs[0];
            String user01 = null;
            for (int i = 1; i < strs.length; i++) {
                mkey.set(fof(strs[0], strs[i]));
                mval.set(0);
                context.write(mkey, mval);

                for (int j = i + 1; j < strs.length; j++) {
                    Thread.sleep(context.getConfiguration().getInt("sleep", 0));
                    mkey.set(fof(strs[i], strs[j]));
                    mval.set(1);
                    context.write(mkey, mval);

                }
            }
        }

        /**
         *
         * @param str1
         * @param str2
         * @return 同样的两个用户 ，返回一致的排序结果
         */
        public static String fof(String str1  , String str2){

            if(str1.compareTo(str2) > 0){
                //hello,hadoop
                return str2+":"+str1;
                //hadoop:hello
            }
            //hadoop,hello
            return str1+":"+str2;
            //hadoop:hello
        }
    }

    static  class FReducer  extends  Reducer<Text, IntWritable, Text, Text> {
        Text rval = new Text();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> vals,  Context context)
                throws IOException, InterruptedException {
            //hadoop:hello  1
            //hadoop:hello  0
            //hadoop:hello  1
            //hadoop:hello  1
            int sum=0;
            int flg=0;
            for (IntWritable v : vals) {
                if(v.get()==0){
                    //hadoop:hello  0
                    flg=1;
                }
                sum+=v.get();
            }
            if(flg==0){
                rval.set(sum+"");
                context.write(key, rval);
            }
        }
    }




    public static void main(String[] args) throws Exception {


        Configuration conf = new Configuration(true);

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

       // conf.set("sleep", otherArgs[1]);

        Job job = Job.getInstance(conf);
        job.setJarByClass(MyFoF.class);

        Path input = new Path(otherArgs[0]);
        FileInputFormat.addInputPath(job, input );

        Path output = new Path(otherArgs[1]);
        if(output.getFileSystem(conf).exists(output)){
            output.getFileSystem(conf).delete(output,true);
        }
        FileOutputFormat.setOutputPath(job, output );


        job.setMapperClass(FMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(FReducer.class);

        job.waitForCompletion(true);


    }



}
