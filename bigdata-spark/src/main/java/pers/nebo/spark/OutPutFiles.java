package pers.nebo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * @auther nebofeng
 * @email nebofeng@gmail.com
 * @date 2018/11/22
 * @des : spark 输出文件

 */
public class OutPutFiles {
    public static void main(String[] args) {
        SparkConf sc = new SparkConf();
        JavaSparkContext javaSparkContext = new JavaSparkContext(sc);
        JavaRDD<String> javaRDD = javaSparkContext.textFile("");
        javaRDD.mapToPair(new PairFunction<String, Object, Object>() {
            @Override
            public Tuple2<Object, Object> call(String s) {
                return null;
            }
        }).reduceByKey(new Function2<Object, Object, Object>() {
            @Override
            public Object call(Object v1, Object v2) {
                return null;
            }
        }).saveAsTextFile("targetfile");

    }
}
