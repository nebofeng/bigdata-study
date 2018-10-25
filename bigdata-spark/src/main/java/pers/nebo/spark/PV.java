package pers.nebo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

/**
 * @auther nebofeng@gmail.com
 * @date 2018/9/25 11:05
 */
public class PV {
	public static void main(String[] args) {
		SparkConf  conf = new SparkConf().setMaster("local").setAppName("PVTest");
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("WARN");
		count(sc);
 	}

 	//park 求出 最高气温
 	static void  year_max(JavaSparkContext sc){
		//数据集rdd
		JavaRDD<String>  dataRdd = sc.textFile("file:///E://tmp//a.txt");

		JavaPairRDD<String ,Integer> urlAndOne = dataRdd.filter(new Function<String, Boolean>() {
			public Boolean call(String s) throws Exception {
				return (s.split("/").length>10);
			}
		}).mapToPair((String s)->{  return  new Tuple2(s.split("/")[0],"1");});

	}
    //计算count
	static void count(JavaSparkContext sc){
		JavaRDD<String>  dataRdd = sc.textFile("file:///E://tmp//a.txt");
		Long count =dataRdd.count();
		System.out.println(count);
	}
	//解析log 计算ip个数
	static void  ipNum(JavaSparkContext sc){
		JavaRDD<String>  dataRdd = sc.textFile("");
		JavaRDD<String>  ipRdd  = dataRdd.map(new Function<String, String>() {
			public String call(String s) throws Exception {
				return s.split(".")[0];
			}
		});

		JavaRDD<String> ipRdds = dataRdd.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String s) throws Exception {
				return (Iterator<String>) Arrays.asList(s.split("/"));
			}
		});

	}
	//解析 log TopN
	static Map<String,Integer> topN(JavaSparkContext sc){
		JavaRDD<String>  dataRdd = sc.textFile("");
		JavaPairRDD<String ,Integer> urlAndOne = dataRdd.filter(new Function<String, Boolean>() {
			public Boolean call(String s) throws Exception {
				return (s.split("/").length>10);
			}
		}).mapToPair((String s)->{  return  new Tuple2(s.split("/")[0],"1");});







		JavaPairRDD<String ,Integer> urlAndOne2 = dataRdd.filter( (String s)-> { return s.split("/").length>10; } )
				.mapToPair(new PairFunction<String, String, Integer>(){

			public Tuple2<String, Integer> call(String s) {
				return new Tuple2(s.split("/")[0],1);
			}
		});

		return (Map<String, Integer>) urlAndOne.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer integer, Integer integer2) throws Exception {
				return integer+integer2;
			}
		}).mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {
			public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) {
				return null;
			}
		}).sortByKey().top(5, new Comparator<Tuple2<Integer, String>>() {
			public int compare(Tuple2<Integer, String> o1, Tuple2<Integer, String> o2) {
				if(o1._1>o2._1){
					return 1;
				}else if(o1._1<o2._1){
					return 1;
				}else{
					return 0;
				}
			}
		});

	}
}
