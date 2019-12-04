package pers.nebo.sparksql.dataframe;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
 import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
/**
 * 读取json格式的RDD创建DF
 * @author root
 *
 */
public class CreateDFFromJsonRDD {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("jsonRDD");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		JavaRDD<String> nameRDD = sc.parallelize(Arrays.asList(
					"{'name':'zhangsan','age':\"18\"}",
					"{'name':\"lisi\",\"age\":\"19\"}",
					"{\"name\":\"wangwu\",\"age\":\"20\"}"
				));
		JavaRDD<String> scoreRDD = sc.parallelize(Arrays.asList(
				"{\"name\":\"zhangsan\",\"score\":\"100\"}",
				"{\"name\":\"lisi\",\"score\":\"200\"}",
				"{\"name\":\"wangwu\",\"score\":\"300\"}"
				));

		Dataset<Row> namedf = sqlContext.read().json(nameRDD);
		namedf.show();
		Dataset<Row> scoredf = sqlContext.read().json(scoreRDD);
		scoredf.show();
		
		//SELECT t1.name,t1.age,t2.score from t1, t2 where t1.name = t2.name
		//daframe原生api使用
//		namedf.join(scoredf, namedf.col("name").$eq$eq$eq(scoredf.col("name")))
//		.select(namedf.col("name"),namedf.col("age"),scoredf.col("score")).show();	
		
		//注册成临时表使用
		namedf.registerTempTable("name");
		scoredf.registerTempTable("score");
		/**
		 * 如果自己写的sql查询得到的DataFrame结果中的列会按照 查询的字段顺序返回
		 */
		Dataset<Row> result =
				sqlContext.sql("select name.name,name.age,score.score from name join score on name.name = score.name");
		result.show();
		sc.stop();
	}
}
