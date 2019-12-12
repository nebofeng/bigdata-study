package pers.nebo.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @auther nebofeng
 * @email nebofeng@gmail.com
 * @date 2018/11/12
 * @des :  基于Transfrom的黑名单过滤功能
 */
public class TransformBlackList {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("TransformBlackList");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        //黑名单数据
        List<Tuple2<String ,Boolean>>  blackList = new ArrayList<Tuple2<String, Boolean>>();
        blackList.add(new Tuple2<>("tom",true));
        JavaPairRDD<String,Boolean> blacklistRdd = jssc.sparkContext().parallelizePairs(blackList);
        //广告log
        JavaReceiverInputDStream<String> adClickLog = jssc.socketTextStream("",9999);
        JavaPairDStream<String,String> userAdClickLogDStream=adClickLog.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
               // 返回    user ，log  格式的 pair
                return null;
            }
        });
         // user 为key  join后返回 key  some(value)
        JavaDStream<String> log = userAdClickLogDStream.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaPairRDD<String, String> userADlog) throws Exception {
               //左外连接，不是黑名单的user也还是会保存下来。
               JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> joinRdd = userADlog.leftOuterJoin(blacklistRdd);
               //连接之后执行filter算子
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> filterRdd =  joinRdd.filter(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> v1) throws Exception {
                        //v1 为 用户日志。是否在黑名单。若存在。且不为空。则 在黑名单中。
                        return (v1._2._2().isPresent())&& v1._2._2.get()? false: true;
                    }
                });
                return filterRdd.map(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, String>() {
                    @Override
                    public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> v1) throws Exception {
                        return v1._2._1;
                    }
                });
            }
        });
        log.print();
        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }
        jssc.stop();

    }
}
