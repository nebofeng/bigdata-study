package pers.nebo.mrtohbase;

/**
 * @ author fnb
 * @ email nebofeng@gmail.com
 * @ date  2019/12/17
 * @ des :
 */
import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


/**
 *
 * mr to hbase 的输入与输出 有 job设置 ，
 * tableMapper 为从hbase 读取数据
 * tableReduce 为往Hbase 写入数据
 *   写入的表名 通过job 设置为 tablename (一个) 则 reduce 函数中 为 NullWritable.get
 *   写入的表名 为多个 则 job 设置为 MultiTableOutputFormat  通过 reduce 函数指定tableName
 *
 *
 *
 */
public class MR2Hbase {

    public static class Map extends Mapper<LongWritable, Text, Text, LongWritable>{

        //该步骤同一般Map程序
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            //将读入行转化为字符串
            String line = value.toString();
            //切分字符串
            String[] words = StringUtils.split(line," ");
            //将单词写入context
            for(String word:words) {
                context.write(new Text(word), new LongWritable(1));
            }
        }

    }

    public static class Reduce extends TableReducer<Text, LongWritable, NullWritable>{

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values,Context context)
                throws IOException, InterruptedException {
            long count =0;
            for(LongWritable value : values) {
                count += value.get();
            }
            //实例化Put，将单词作为主键
            Put put = new Put(Bytes.toBytes(key.toString()));
            //列族为content，key为result，value为count
            put.addColumn(Bytes.toBytes("content"), Bytes.toBytes("result"), Bytes.toBytes(String.valueOf(count)));
            context.write(NullWritable.get(), put);
        }

    }

    //创建表
    public static void createTable(String tablename) throws Exception{

        //设置配置文件
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "master:2181,slave1:2181,slave2:2181");

        Connection  conn = ConnectionFactory.createConnection(conf);

        //hbase客户端实例
        Admin admin = conn.getAdmin();

        //指定表名
        TableName name = TableName.valueOf(tablename);

        //向表描述里添加表名
        HTableDescriptor desc = new HTableDescriptor(name);

        //指定列族名和版本数
        HColumnDescriptor content = new HColumnDescriptor("content");
        content.setMaxVersions(3);

        //向表描述里添加列族
        desc.addFamily(content);
        //判断表是否存在
        if(admin.tableExists(name)){
            System.out.println("table exists,trying recreate table !");
            admin.disableTable(name);
            admin.deleteTable(name);
        }
        System.out.println("Create new table: "+ tablename);

        //创建表
        admin.createTable(desc);

        admin.close();
    }

    public static void main(String[] args) throws Exception{

        String tablename = "WC";
        //创建表
        createTable(tablename);

        //配置文件
        Configuration conf = new Configuration();
        conf.set(TableOutputFormat.OUTPUT_TABLE, tablename);
        Job job = Job.getInstance(conf);

        //设置整个job所调用类的jar包路径
        job.setJarByClass(MR2Hbase.class);

        //设置该作业所使用的mapper和reducer类
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        //指定mapper输出数据的k-v类型，和reduce输出类型一样，可缺省
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //指定reduce输出到Hbase
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //指定输入数据存放路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        //指定输出到HBase
        job.setOutputFormatClass(TableOutputFormat.class);

        //将job提交给集群运行，参数为true表示提示运行进度
        System.exit(job.waitForCompletion(true)?0:1);

    }

}