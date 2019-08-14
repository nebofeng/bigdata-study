package pers.nebo.sparkcore.mapjointest;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ author fnb
 * @ email nebofeng@gmail.com
 * @ date  2019/8/14
 * @ des :  reduce join 通过 广播变量改为 map join
 */
public class MapJoinTestJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("MapJoinTest");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String, String>> list1 = Arrays.asList(
                new Tuple2<String,String>("001","令狐冲"),
                new Tuple2<String,String>("002","任盈盈")
        );
        List<Tuple2<String, String>> list2 = Arrays.asList(
                new Tuple2<String,String>("001","一班"),
                new Tuple2<String,String>("002","二班")
        );
        JavaRDD<Tuple2<String, String>> list1RDD = sc.parallelize(list1);
        JavaRDD<Tuple2<String, String>> list2RDD = sc.parallelize(list2);
        List<Tuple2<String, String>> rdd1data = list1RDD.collect();
        final Broadcast<List<Tuple2<String, String>>> rdd1braodcast = sc.broadcast(rdd1data);
        JavaPairRDD<String, Tuple2<String, String>> resultRDD = list2RDD.mapToPair(new PairFunction<Tuple2<String,String>, String,Tuple2<String,String>>() {

            public Tuple2<String, Tuple2<String, String>> call(
                    Tuple2<String, String> t) throws Exception {
                List<Tuple2<String, String>> rdd1data = rdd1braodcast.value();
                Map<String, String> rdd1dataMap = new HashMap<String,String>();
                for(Tuple2<String,String> data:rdd1data){
                    rdd1dataMap.put(data._1, data._2);
                }
                //rdd2 key value
                String key=t._1;
                String value=t._2;
                String rdd1value=rdd1dataMap.get(key);
                return new Tuple2<String, Tuple2<String, String>>(key,new Tuple2<String, String>(value,rdd1value));
            }
        });

        resultRDD.foreach(new VoidFunction<Tuple2<String,Tuple2<String,String>>>() {

            public void call(Tuple2<String, Tuple2<String, String>> t) throws Exception {
                System.out.println(t._1 + "  "+ t._2._1 +"  "+ t._2._2);

            }
        });
    }
}
