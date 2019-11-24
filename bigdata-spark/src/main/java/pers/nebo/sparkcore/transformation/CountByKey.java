package pers.nebo.sparkcore.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

/**
 * @ author fnb
 * @ email nebofeng@gmail.com
 * @ date  2019/11/25
 * @ des :
 */
public class CountByKey {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //转为 k v 使用  parallelizePairs
        JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, Integer>("zhangsan", 10),
                new Tuple2<String, Integer>("zhangsan", 100),
                new Tuple2<String, Integer>("lisi", 20),
                new Tuple2<String, Integer>("lisi", 20),
                new Tuple2<String, Integer>("wangwu", 300)
        ));
        Map<Tuple2<String, Integer>, Long> map = rdd1.countByValue();
        Set<Map.Entry<Tuple2<String, Integer>, Long>> set = map.entrySet();
        for(Map.Entry<Tuple2<String, Integer>, Long> entry : set){
            Tuple2<String, Integer> key = entry.getKey();
            Long value = entry.getValue();
            System.out.println("key = "+key+",value = "+value);
        }
//        Map<String, Long> map = rdd1.countByKey();
//        Set<Map.Entry<String, Long>> set = map.entrySet();
//        for(Map.Entry<String, Long> entry : set){
//            String key = entry.getKey();
//            Long value = entry.getValue();
//            System.out.println("key = "+key+",value = "+value);
//        }


//        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
//        Integer reduce = rdd1.reduce(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer v1, Integer v2) throws Exception {
//                return v1 + v2;
//            }
//        });
//        System.out.println(reduce);

        /**
         * reduce
         * countByKey
         * countByValue
         */
    }
}
