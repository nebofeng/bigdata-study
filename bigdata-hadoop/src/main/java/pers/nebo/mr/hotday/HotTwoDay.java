package pers.nebo.mr.hotday;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @ author fnb
 * @ email nebofeng@gmail.com
 * @ date  2019/10/24
 * @ des : 找到一组格式为：1949-10-01 14:21:02	34c
 *  找出每个月温度最大的两天
 */

/*

思路一： mr job  将key 封装为 year-month的格式   传递给 reduce
        reduce 中遍历每个key 的值，取出温度，进行比较。 遍历完成之后输出


思路二： 利用key的排序， 实现自定义分组 。根据 ，year、month、温度 来分区。 温度大的排前面。
        这样每个reduce处理的时候，第一个数据符合要求， 再取出 year、month 、day 与第一个不同的第二个数据就可以了

        代码实现的是思路二

 */
public class HotTwoDay {


    static class  HotDayMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, HotDay,Text>{

        @Override
        protected void map(LongWritable key, Text value,Context context)
                throws IOException, InterruptedException {

            HotDay hDay=new HotDay();
            Text vwd =new Text();
            //value:  1949-10-01 14:21:02	34c
            try {
                String[] strs = StringUtils.split(value.toString(), '\t');

                SimpleDateFormat  sdf = new SimpleDateFormat("yyyy-MM-dd");
                Date date = null;

                date = sdf.parse(strs[0]);

                Calendar  cal = Calendar.getInstance();
                cal.setTime(date);

                hDay.setYear(cal.get(Calendar.YEAR));
                hDay.setMonth(cal.get(Calendar.MONTH)+1);
                //Java通过cal.get(Calendar.MONTH)比真实月份少了一个月,这里月份是从0开始计算的，也就是说，月份是从0—11。

                hDay.setDay(cal.get(Calendar.DAY_OF_MONTH));

                int wd  = Integer.parseInt(strs[1].substring(0, strs[1].length()-1));
                hDay.setWd(wd);
                vwd.set(wd+"");

                context.write(hDay, vwd);

            } catch (ParseException e) {

                e.printStackTrace();
            }
        }
    }



    static  class  HotDayReducer extends Reducer<HotDay,Text,Text,Text>{

        Text rkey = new Text();
        Text rval = new Text();

        @Override
        protected void reduce(HotDay key, Iterable<Text> vals, Context context)
                throws IOException, InterruptedException {

            int flg=0;
            int day=0;

            for (Text v : vals) {
                if(flg==0){
                    day=key.getDay();

                    rkey.set(key.getYear()+"-"+key.getMonth()+"-"+key.getDay());
                    rval.set(key.getWd()+"");
                    context.write(rkey,rval );
                    flg++;
                }
                if(flg!=0 && day != key.getDay()){

                    rkey.set(key.getYear()+"-"+key.getMonth()+"-"+key.getDay());
                    rval.set(key.getWd()+"");
                    context.write(rkey,rval );
                    break;
                }
             }
        }
    }

    static class HotDayPartitioner  extends Partitioner<HotDay, Text> {

        @Override
        public int getPartition(HotDay key, Text value, int numPartitions) {
            return key.getYear() % numPartitions;
        }


    }

    /**
     * 实现天气年月正序， 温度倒序
     */
    static  class HotDaySortComparator  extends WritableComparator {
        HotDay day1= null;
        HotDay day2= null;
        public  HotDaySortComparator(){
            super(HotDay.class,true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            day1=(HotDay) a;
            day2=(HotDay) b;
            int flagy=Integer.compare(day1.getYear(),day2.getYear());
            if(flagy==0){
                int flagm=Integer.compare(day1.getMonth(),day2.getMonth());
                if(flagm==0){
                    //年、月 正序，温度倒序
                    return - Integer.compare(day1.getWd(),day2.getWd());
                }
                return flagm;
            }
            return flagy;
        }
    }

    /**
     * 分组使得，year ，month 相同的分到一个组， 排序使得 一个组内，温度倒序排列
     */
    static  class HotDayGroupingComparator  extends WritableComparator {


        HotDay day1= null;
        HotDay day2= null;
        public  HotDayGroupingComparator(){
            super(HotDay.class,true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            day1=(HotDay) a;
            day2=(HotDay) b;
            int flagy=Integer.compare(day1.getYear(),day2.getYear());
            if(flagy==0){
                return  Integer.compare(day1.getMonth(),day2.getMonth());
            }
            return flagy;
        }

    }


    public static void main(String[] args)  throws  Exception{
        //1,conf
        Configuration conf = new Configuration(true);
        //2,job
        Job job=Job.getInstance(conf);
        job.setJarByClass(HotTwoDay.class);
        //3,input,output
        Path input =new Path("");
        Path output = new Path("");
        FileInputFormat.addInputPath(job,input);
        if(output.getFileSystem(conf).exists(output)){
            output.getFileSystem(conf).delete(output,true);
        }
        FileOutputFormat.setOutputPath(job, output );
        //4,map
        job.setMapperClass(HotDayMapper.class);
        job.setMapOutputKeyClass(HotDay.class);
        job.setMapOutputValueClass(Text.class);
        //5,reduce
        job.setReducerClass(HotDayReducer.class);
        //6,other:sort,part..,group...
        job.setPartitionerClass(HotDayPartitioner.class);
        job.setSortComparatorClass(HotDaySortComparator.class);
        job.setGroupingComparatorClass(HotDayGroupingComparator.class);
        //7,submit
        job.waitForCompletion(true);

    }
}
