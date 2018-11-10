package pers.nebo.sparkstreaming;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * @auther nebofeng
 * @email nebofeng@gmail.com
 * @date 2018/11/5
 * @des :UpdateStateByKeyWordCount 使用 updatestatebykey 完成wordcount
 */
public class UpdateStateByKeyWordCount {
    public static Logger log = Logger.getLogger(UpdateStateByKeyWordCount.class);
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("KafkaWordCount");



        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        JavaReceiverInputDStream  lines  = jssc.socketTextStream("118.89.61.19", 8888);
        JavaDStream<String>  datas =  lines.flatMap(new FlatMapFunction<String,String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }

        });


        JavaPairDStream<String  ,Integer> words = datas.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s,1);
            }
        });

        //到这里 。reducebykey 可以得到 wordcount，但是如果想要得到全局（从应用启动开始）的 wordccount ，就需要用到redis之类的存储
        //updateStatebykey 可以通过spark维护一份全局变量
        JavaPairDStream<String ,Integer> wordCount = words.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
           //对于每个单词.每个批次的操作.都会调用这个函数
            //第一个参数 是这个batch的 数值  ，第二个参数是之前的数值
            @Override
            public Optional<Integer> call(List<Integer> v1, Optional<Integer> v2) throws Exception {
                   Integer newvalue =0;
                   //存在则累加
                   if(v2.isPresent()){
                       newvalue=v2.get();
                   }
                   for(Integer integer:v1){
                       newvalue+=integer;
                   }
                   return  Optional.of(newvalue);
            }
        });

        wordCount.print();

       // 使用updatestatebykey 必须启动checkpoint
        jssc.checkpoint("/TEMP");

        jssc.start();

        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
            jssc.stop();
        }


    }

}
