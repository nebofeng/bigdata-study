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
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
/**
 * UDAF 用户自定义聚合函数
 * @author root
 */
public class UDAF {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("udaf");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		JavaRDD<String> parallelize = sc.parallelize(
				Arrays.asList("zhangsan","lisi","wangwu","zhangsan","zhangsan","lisi","lisi","lisi","lisi"));
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
		
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> df = sqlContext.createDataFrame(rowRDD, schema);
		df.registerTempTable("user");
		/**
		 * 注册一个UDAF函数,实现统计相同值得个数
		 * 注意：这里可以自定义一个类继承UserDefinedAggregateFunction类也是可以的
		 */
		sqlContext.udf().register("StringCount",new UserDefinedAggregateFunction() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;
			
			/**
			 * 初始化一个内部的自己定义的值,在Aggregate之前每组数据的初始化结果
			 */
			@Override
			public void initialize(MutableAggregationBuffer buffer) {
				buffer.update(0, 0);
			}
			
			/**
			 * 更新 可以认为一个一个地将组内的字段值传递进来 实现拼接的逻辑
			 * buffer.getInt(0)获取的是上一次聚合后的值
			 * 相当于map端的combiner，combiner就是对每一个map task的处理结果进行一次小聚合 
			 * 大聚和发生在reduce端.
			 * 这里即是:在进行聚合的时候，每当有新的值进来，对分组后的聚合如何进行计算
			 */
			@Override
			public void update(MutableAggregationBuffer buffer, Row arg1) {
				buffer.update(0, buffer.getInt(0)+1);
				
			}
			/**
			 * 合并 update操作，可能是针对一个分组内的部分数据，在某个节点上发生的 但是可能一个分组内的数据，会分布在多个节点上处理
			 * 此时就要用merge操作，将各个节点上分布式拼接好的串，合并起来
			 * buffer1.getInt(0) : 大聚合的时候 上一次聚合后的值       
			 * buffer2.getInt(0) : 这次计算传入进来的update的结果
			 * 这里即是：最后在分布式节点完成后需要进行全局级别的Merge操作
			 */
			@Override
			public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
				buffer1.update(0, buffer1.getInt(0) + buffer2.getInt(0));
			}
			/**
			 * 在进行聚合操作的时候所要处理的数据的结果的类型
			 */
			@Override
			public StructType bufferSchema() {
				return DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("bffer22", DataTypes.IntegerType, true)));
			}
			/**
			 * 最后返回一个和dataType方法的类型要一致的类型，返回UDAF最后的计算结果
			 */
			@Override
			public Object evaluate(Row row) {
				return row.getInt(0);
			}
			/**
			 * 指定UDAF函数计算后返回的结果类型
			 */
			@Override
			public DataType dataType() {
				return DataTypes.IntegerType;
			}
			/**
			 * 指定输入字段的字段及类型
			 */
			@Override
			public StructType inputSchema() {
				return DataTypes.createStructType(
						Arrays.asList(
								DataTypes.createStructField("name111", DataTypes.StringType, true)
						));
			}
			/**
			 * 确保一致性 一般用true,用以标记针对给定的一组输入，UDAF是否总是生成相同的结果。
			 */
			@Override
			public boolean deterministic() {
				return true;
			}
			
		});
		
		sqlContext.sql("select name ,StringCount(name) as strCount from user group by name").show();
		
		
		sc.stop();
	}
}
