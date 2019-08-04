package pers.nebo.sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @ author fnb
 * @ email nebofeng@gmail.com
 * @ date  2019/8/2
 * @ des : spark 算子 java 版本
 */
public class TransformationOperationJava {

    public static void sortByKey(){
        SparkConf conf = new SparkConf();
        //本地运行，设置master为local
        conf.setMaster("local");
        conf.setAppName("reducebykey");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String,Integer>> list = Arrays.asList(
                new Tuple2<String,Integer>("key1",1),
                new Tuple2<String,Integer>("key1",12),
                new Tuple2<String,Integer>("key4",11),
                new Tuple2<String,Integer>("key2",11),
                new Tuple2<String,Integer>("key3",1)
        );
        //bykey 用到的是pairs形式元素
        JavaPairRDD<String,Integer> listRDD =sc.parallelizePairs(list);

        listRDD.sortByKey(false)
                .foreach(
                        new VoidFunction<Tuple2<String, Integer>>() {
                            @Override
                            public void call(Tuple2<String, Integer> t) throws Exception {
                                System.out.println(t._1+"=="+t._2);
                            }
                        }
                );




    }





    public static void reduceBykey(){
        SparkConf conf = new SparkConf();
        //本地运行，设置master为local
        conf.setMaster("local");
        conf.setAppName("reducebykey");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String,Integer>> list = Arrays.asList(
                new Tuple2<String,Integer>("key1",1),
                new Tuple2<String,Integer>("key1",12),
                new Tuple2<String,Integer>("key2",11),
                new Tuple2<String,Integer>("key2",11),
                new Tuple2<String,Integer>("key3",1)
        );
        //bykey 用到的是pairs形式元素
        JavaPairRDD<String,Integer> listRDD =sc.parallelizePairs(list);

        JavaPairRDD<String,Integer>reduceBykey=listRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });


        reduceBykey.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1+"=="+t._2);
            }
        });




    }




    public static void groupBykey(){
        SparkConf conf = new SparkConf();
        //本地运行，设置master为local
        conf.setMaster("local");
        conf.setAppName("groupbykeytest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String,String>> list = Arrays.asList(
                new Tuple2<String,String>("key1","value1"),
                new Tuple2<String,String>("key1","value2"),
                new Tuple2<String,String>("key2","value2"),
                new Tuple2<String,String>("key2","value3"),
                new Tuple2<String,String>("key3","value3")
        );
        //bykey 用到的是pairs形式元素
        JavaPairRDD<String,String> listRDD =sc.parallelizePairs(list);
        JavaPairRDD<String, Iterable<String>> groupBykeyRDD=listRDD.groupByKey();
        groupBykeyRDD.foreach(
                new VoidFunction<Tuple2<String, Iterable<String>>>() {
                    @Override
                    public void call(Tuple2<String, Iterable<String>> stringIterableTuple2) throws Exception {
                        System.out.println(stringIterableTuple2._1);
                        for(String string:stringIterableTuple2._2){
                            System.out.println(string);
                        }
                    }
                }
        );

    }


    public static void flatMap() {

        SparkConf conf = new SparkConf();
        //本地运行，设置master为local
        conf.setMaster("local");
        conf.setAppName("maptest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> list = Arrays.asList("1 demo1", "2   demo2");
        JavaRDD<String> listRDD = sc.parallelize(list);
        JavaRDD<String> flatMap = listRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return (Iterator<String>) Arrays.asList(s.split("\t"));
            }
        });
        flatMap.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });


    }


    public static void filter() {
        SparkConf conf = new SparkConf();
        //本地运行，设置master为local
        conf.setMaster("local");
        conf.setAppName("maptest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> list = Arrays.asList(1, 2, 2);
        JavaRDD<Integer> listRDD = sc.parallelize(list);

        JavaRDD<Integer> filter = listRDD.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer v1) throws Exception {
                return v1 % 2 == 0;
            }
        });

        filter.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });


    }


    public static void map() {
        //创建sparkConf
        SparkConf conf = new SparkConf();
        //本地运行，设置master为local
        conf.setMaster("local");
        conf.setAppName("maptest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> list = Arrays.asList("demo1", "demo2", "demo3");
        JavaRDD<String> listRDD = sc.parallelize(list);

        JavaRDD<String> map = listRDD.map(new Function<String, String>() {
            @Override
            public String call(String v1) throws Exception {
                return "hello" + v1;
            }
        });
        map.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }


    public static void main(String[] args) {

//        filter();
//
//        reduceBykey();

        sortByKey();
    }

}
