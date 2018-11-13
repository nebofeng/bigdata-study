package pers.nebo.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.List;

/**
 * @auther nebofeng
 * @email nebofeng@gmail.com
 * @date 2018/11/13
 * @des : 热点搜索词实时统计
 */
public class WindowHotWord {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("KafkaWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        //得到搜索日志  格式为： name ， log
        JavaReceiverInputDStream<String> lines  = jssc.socketTextStream("118.89.61.19", 8888);
        // 搜索日志转为搜索词
        JavaDStream<String> searchWords= lines.map(new Function<String,String>() {

            @Override
            public String call(String v1) throws Exception {
                return null;
            }
        });
        //将搜索词转为 （word ，1 ）的形式

        JavaPairDStream<String,Integer> words = searchWords.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s,1);
            }
        });
        //reducebykeyandwindow 的窗口操作

        JavaPairDStream<String,Integer> reduceWord = words.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        },Durations.seconds(60),Durations.seconds(10));
        //获取60s内的热点搜索词，然后排序取出前三，


        JavaPairDStream<String ,Integer> finalWord = reduceWord.transformToPair(new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>>() {
            @Override
            public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> v1) throws Exception {
                //搜索词与次数的反转
                 JavaPairRDD<Integer,String> countWord = v1.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                     @Override
                     public Tuple2<Integer, String> call(Tuple2<String, Integer> v2) throws Exception {
                         return new Tuple2<>(v2._2,v2._1);
                     }
                 });
               List<Tuple2<Integer, String>> countWordTake =   countWord.sortByKey(false).take(3);
               for(Tuple2 tuple :countWordTake){
                   System.out.println( tuple._2 +"==="+ tuple._1);
               }
                return  v1;
            }
        });
        //触发动作
        finalWord.print();
        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        jssc.stop();

    }
}
