package pers.nebo.sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @ author fnb
 * @ email nebofeng@gmail.com
 * @ date  2019/8/6
 * @ des : spark action操作练习
 */
public class ActionOperationsJava {

    public static void takeSample(){
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("takeSample");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list= Arrays.asList(224,1,2,3,5,8,7);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        List<Integer> takeSample = listRDD.takeSample(true, 2);
        for (Integer integer : takeSample) {
            System.out.println(integer);
        }
    }

    public static void countByKey(){
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("countByKey");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String, Integer>> list = Arrays.asList(
                new Tuple2<String,Integer>("峨眉",30),
                new Tuple2<String,Integer>("武当",40),
                new Tuple2<String,Integer>("峨眉",60),
                new Tuple2<String,Integer>("武当",70)
        );
        JavaPairRDD<String, Integer> listRDD = sc.parallelizePairs(list);
        //JavaRDD<Tuple2<String, Integer>> listRDD = sc.parallelize(list);
        Map<String, Long> countByKey = listRDD.countByKey();
        for(String key:countByKey.keySet()){
            System.out.println(key + ":" + countByKey.get(key));
        }
    }

    public static void saveAsTextFile(){
        /**
         * http://blog.csdn.net/kimyoungvon/article/details/51308651
         *
         *
         * Caused by: java.lang.NullPointerException
         at java.lang.ProcessBuilder.start(ProcessBuilder.java:1010)
         at org.apache.hadoop.util.Shell.runCommand(Shell.java:482)
         at org.apache.hadoop.util.Shell.run(Shell.java:455)
         at org.apache.hadoop.util.Shell$ShellCommandExecutor.execute(Shell.java:715)
         at org.apache.hadoop.util.Shell.execCommand(Shell.java:808)
         *
         *
         */
        SparkConf conf = new SparkConf();
        //conf.setMaster("local[3]");
        conf.setAppName("saveAsTextFile");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> lista= Arrays.asList(1,2,3,4);
        List<Integer> listb= Arrays.asList(4,5,6,7);
        JavaRDD<Integer> listaRDD = sc.parallelize(lista);
        JavaRDD<Integer> listbRDD = sc.parallelize(listb);
        JavaRDD<Integer> union = listaRDD.union(listbRDD);
        union.repartition(1).saveAsTextFile("/union");
    }


    public static void takeOrdered(){
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("takeOrdered");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list= Arrays.asList(224,1,2,3,5,8,7);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        List<Integer> takeOrdered = listRDD.takeOrdered(3);
        for (Integer integer : takeOrdered) {
            System.out.println(integer);
        }

        List<Integer> top = listRDD.top(3);
        for (Integer integer : top) {
            System.out.println(integer);
        }
    }

    public static void count(){
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("count");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list= Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        long count = listRDD.count();
        System.out.println(count);
    }


    //topN
    public static void take(){
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("take");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list= Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        List<Integer> take = listRDD.take(3);
        for (Integer integer : take) {
            System.out.println(integer);
        }
    }


    public static void collect(){
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("collect");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> lista= Arrays.asList(1,2,3,4);
        List<Integer> listb= Arrays.asList(4,5,6,7);
        JavaRDD<Integer> listaRDD = sc.parallelize(lista);
        JavaRDD<Integer> listbRDD = sc.parallelize(listb);
        JavaRDD<Integer> union = listaRDD.union(listbRDD);

        //收集，并返回，谨慎使用，数据量大，容易造成oom
        List<Integer> collect = union.collect();
        for (Integer integer : collect) {
            System.out.println(integer);
        }
    }

    public static void reduce(){
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("reduce");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list= Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        //reduce是一个action的操作
        Integer reduce = listRDD.reduce(new Function2<Integer, Integer, Integer>() {

            public Integer call(Integer v1, Integer v2) throws Exception {

                return v1+v2;
            }
        });
        System.out.println(reduce);
    }


    public static void main(String[] args) {

    }
}
