package pers.nebo.sparksql.udf_udaf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
 import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
/**
 * UDF 用户自定义函数
 * @author root
 *
 */
public class UDF {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("udf");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		JavaRDD<String> parallelize = sc.parallelize(Arrays.asList("zhangsan","lisi","wangwu"));
		JavaRDD<Row> rowRDD = parallelize.map(new Function<String, Row>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Row call(String s) throws Exception {
				return RowFactory.create(s);
			}
		});
		
		/**
		 * 动态创建Schema方式加载DF
		 */
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("name", DataTypes.StringType,true));
		StructType schema = DataTypes.createStructType(fields);
		
		Dataset<Row> df = sqlContext.createDataFrame(rowRDD,schema);
		
		df.registerTempTable("user");
		
		/**
		 * 根据UDF函数参数的个数来决定是实现哪一个UDF  UDF1，UDF2。。。。UDF1xxx
		 */
		sqlContext.udf().register("StrLen", new UDF1<String,Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(String name) throws Exception {
				return name.length();
			}
		}, DataTypes.IntegerType);
		sqlContext.sql("select name ,StrLen(name) as length from user").show();
		
//		sqlContext.udf().register("StrLen",new UDF2<String, Integer, Integer>() {
//
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Integer call(String t1, Integer t2) throws Exception {
//				return t1.length()+t2;
//			}
//		} ,DataTypes.IntegerType );
//		sqlContext.sql("select name ,StrLen(name,10) as length from user").show();
		
		
		sc.stop();
		
	}
}
