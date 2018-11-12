package pers.nebo.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
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
                return null;
            }
        });

        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        jssc.stop();

    }
}
