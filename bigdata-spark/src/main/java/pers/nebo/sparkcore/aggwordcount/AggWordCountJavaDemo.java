package pers.nebo.sparkcore.aggwordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.rdd.RDD;
import scala.Function1;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

/**
 * @ author fnb
 * @ email nebofeng@gmail.com
 * @ date  2019/8/14
 * @ des : 数据倾斜，先打散，再聚合 Java 版本
 */
public class AggWordCountJavaDemo {

    public static void main(String[] args) {
        SparkConf conf =  new SparkConf().setMaster("local").setAppName("aggwordcount");
        JavaSparkContext sc = new JavaSparkContext(conf);

         JavaPairRDD<String,Integer> javaRDD= sc.textFile("").flatMap(new FlatMapFunction<String, String>() {
             @Override
             public Iterator<String> call(String s) throws Exception {
                 return Arrays.asList(s.split("\t")).iterator();
             }
         }).mapToPair(new PairFunction<String, String, Integer>() {
             @Override
             public Tuple2<String, Integer> call(String s) throws Exception {
                 return new Tuple2<>(s,1);
             }
         });


        /**
         * 给rdd中的每一个key的前缀都打上随机数
         */
        JavaPairRDD<String, Integer> prefixRDD = javaRDD.mapToPair(new PairFunction<Tuple2<String,Integer>, String, Integer>() {

            public Tuple2<String, Integer> call(Tuple2<String, Integer> t)
                    throws Exception {
                Random random = new Random();
                int prefix = random.nextInt(4);
                return  new Tuple2<String, Integer>(prefix+"_"+t._1,t._2);
            }

        });
        /**
         * 进行局部聚合
         */
        JavaPairRDD<String, Integer> aggRDD = prefixRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {

            public Integer call(Integer v1, Integer v2) throws Exception {
                // TODO Auto-generated method stub
                return v1+v2;
            }
        });
        /**
         * 去除rdd中每个key的前缀
         */
        JavaPairRDD<String, Integer> removePrefixRDD = aggRDD.mapToPair(new PairFunction<Tuple2<String,Integer>,String, Integer>() {

            public Tuple2<String, Integer> call(Tuple2<String, Integer> t)
                    throws Exception {
                String key = t._1.split("_")[1];
                return new Tuple2<String, Integer>(key,t._2);
            }
        });
        /**
         * 进行全局聚合
         */

        removePrefixRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {

            public Integer call(Integer v1, Integer v2) throws Exception {
                // TODO Auto-generated method stub
                return v1+v2;
            }

        }).foreach(new VoidFunction<Tuple2<String,Integer>>() {

            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1 + "    => "+t._2);

            }
        });



    }
}
