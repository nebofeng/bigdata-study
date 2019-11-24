package pers.nebo.sparkcore.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @ author fnb
 * @ email nebofeng@gmail.com
 * @ date  2019/11/24
 * @ des :
 */
public class MapPartitionWithIndex {
    public static void main(String[] args) {
        SparkConf conf= new SparkConf();
        conf.setMaster("local");
        conf.setAppName("test");

        JavaSparkContext jssc= new JavaSparkContext(conf);

        JavaRDD<String> rdd1= jssc.parallelize(Arrays.asList(
                "love1", "love2", "love3", "love4",
                "love5", "love6", "love7", "love8",
                "love9", "love10", "love11", "love12"
        ), 3);

        JavaRDD<String> rdd2 = rdd1.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<String> iter) throws Exception {
                List<String> list = new ArrayList<>();
                while (iter.hasNext()) {
                    String currOne = iter.next();
                    list.add("rdd1 partition index = 【" + index + "】,value = 【" + currOne + "】");
                }
                return list.iterator();
            }
            //开始是没有分区器信息的，所以true 或者false 都一样 scala 默认是false
        }, false);

        /**
         * repartition +coalesce
         */
        JavaRDD<String> rdd3  = rdd2.coalesce(2);
        JavaRDD<String> rdd4 = rdd3.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<String> iter) throws Exception {
                List<String> list = new ArrayList<>();
                while (iter.hasNext()) {
                    String currOne = iter.next();
                    list.add("rdd3 partition index = 【" + index + "】,value = 【" + currOne + "】");
                }
                return list.iterator();
            }
        }, false);


        List<String> result = rdd4.collect();

        for(String s:result){
            System.out.println(s);
        }

    }
}
