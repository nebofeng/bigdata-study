package pers.nebo.sparkstreaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.codehaus.janino.Java;
import scala.Tuple2;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * @auther nebofeng
 * @email nebofeng@gmail.com
 * @date 2018/10/29
 * @des :
 */
public class KafkaWordCount {
    public static Logger log = Logger.getLogger(KafkaWordCount.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("KafkaWordCount");

        //获取配置中的topic
        final Properties serverProps   = new Properties();
          // 使用ClassLoader加载properties配置文件生成对应的输入流
           InputStream in = KafkaWordCount.class.getClassLoader().getResourceAsStream("kafkaconfig.properties");
           // 使用properties对象加载输入流
        try {
            serverProps.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        log.info("topic" + "==="+serverProps.getProperty("topic"));
        System.out.println(serverProps.getProperty("topic"));

        final String topic = "kafkawordcount";
        Set<String> topicSet = new HashSet();
        topicSet.add(topic);

        //组合kafka参数
        final Map<String, Object> kafkaParams = new HashMap();
        kafkaParams.put("bootstrap.servers", "139.199.172.112:9092,123.207.241.42:9092,118.89.61.19:9092");
        kafkaParams.put("group.id", "kafkatest");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", true);

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        jssc.checkpoint("E:/TEMP");

        JavaInputDStream<ConsumerRecord<String, String>> lines = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topicSet, kafkaParams)
        );

        JavaDStream<String> wordCount =  lines.flatMap(new FlatMapFunction<ConsumerRecord<String, String>, String>() {
            @Override
            public Iterator<String> call(ConsumerRecord<String, String> tuple) throws Exception {

//                return Lists.newArrayList(tuple.value().split(" ")).iterator();
                return Arrays.asList(tuple.value().split(" ")).iterator();
            }
        });

        JavaPairDStream<String , Integer> pairs = wordCount.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairDStream<String ,Integer>  count = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer num1, Integer num2) throws Exception {
                return num1+num2;
            }
        });

        count.print();
        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }
      jssc.close();
    }
}
