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

    /**
     * 求rdd并集，但是不去重
     */
    public static void union(){
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("union");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> lista= Arrays.asList(1,2,3,4);
        List<Integer> listb= Arrays.asList(4,5,6,7);
        JavaRDD<Integer> listaRDD = sc.parallelize(lista);
        JavaRDD<Integer> listbRDD = sc.parallelize(listb);
        JavaRDD<Integer> union = listaRDD.union(listbRDD);
        union.foreach(new VoidFunction<Integer>() {

            public void call(Integer t) throws Exception {
                // TODO Auto-generated method stub
                System.out.println(t);
            }

        });
    }



    /**
     * 这个实现根据两个要进行合并的两个RDD操作,生成一个CoGroupedRDD的实例,
     * 这个RDD的返回结果是把相同的key中两个RDD分别进行合并操作,最后返回的RDD的value是一个Pair的实例,
     * 这个实例包含两个Iterable的值,第一个值表示的是RDD1中相同KEY的值,第二个值表示的是RDD2中相同key的值.
     */

    public static void cogroup(){
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("cogroup");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<Integer, String>> listname = Arrays.asList(
                new Tuple2<Integer, String>(1,"东方不败"),
                new Tuple2<Integer, String>(2,"岳不群"),
                new Tuple2<Integer, String>(3,"令狐冲")
        );

        List<Tuple2<Integer, Integer>> listscores = Arrays.asList(
                new Tuple2<Integer, Integer>(1,99),
                new Tuple2<Integer, Integer>(2,80),
                new Tuple2<Integer, Integer>(3,85),
                new Tuple2<Integer, Integer>(1,98),
                new Tuple2<Integer, Integer>(2,79),
                new Tuple2<Integer, Integer>(3,84)
        );
        JavaPairRDD<Integer, String> listnameRDD = sc.parallelizePairs(listname);
        JavaPairRDD<Integer, Integer> listscoresRDD = sc.parallelizePairs(listscores);
        //<1,tuple2<"东方不败" , {99,98}>>
        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> cogroup = listnameRDD.cogroup(listscoresRDD);
        cogroup.foreach(new VoidFunction<Tuple2<Integer,Tuple2<Iterable<String>,Iterable<Integer>>>>() {

            public void call(
                    Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t)
                    throws Exception {
                System.out.println("编号："+t._1);
                //	Iterator<String> names = t._2._1.iterator();
                System.out.println("名字集合："+t._2._1);
                //	Iterator<Integer> scores = t._2._2.iterator();
                //while
                System.out.println("分数单"+ t._2._2);

            }

        });

    }


    public static void join(){
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("join");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<Integer, String>> listname = Arrays.asList(
                new Tuple2<Integer, String>(1,"东方不败"),
                new Tuple2<Integer, String>(2,"岳不群"),
                new Tuple2<Integer, String>(3,"令狐冲")
        );

        List<Tuple2<Integer, Integer>> listscores = Arrays.asList(
                new Tuple2<Integer, Integer>(1,99),
                new Tuple2<Integer, Integer>(2,80),
                new Tuple2<Integer, Integer>(3,85)
        );
        JavaPairRDD<Integer, String> listnameRDD = sc.parallelizePairs(listname);
        JavaPairRDD<Integer, Integer> listscoresRDD = sc.parallelizePairs(listscores);
        JavaPairRDD<Integer, Tuple2<String, Integer>> join = listnameRDD.join(listscoresRDD);
        join.foreach(new VoidFunction<Tuple2<Integer,Tuple2<String,Integer>>>() {

            public void call(Tuple2<Integer, Tuple2<String, Integer>> t)
                    throws Exception {
                System.out.println("编号："+t._1);
                System.out.println("姓名："+t._2._1);
                System.out.println("分数："+t._2._2);

            }

        });
    }

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
