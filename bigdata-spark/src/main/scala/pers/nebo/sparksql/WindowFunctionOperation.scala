package pers.nebo.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/8/27
  * @ des : 开窗函数的使用
  */
object WindowFunctionOperation {
  def main(args: Array[String]): Unit = {

  }

  def window_old(){
    val conf = new SparkConf().setAppName("WindowFunctionOperation")
    val sc = new SparkContext(conf);
    val hiveContext = new HiveContext(sc);
    hiveContext.sql("use xtwy");
    hiveContext.sql("DROP TABLE IF EXISTS result");
    hiveContext.sql("create table result(ipaddress string,category string,product string,price int) row format delimited fields terminated by '\t' ");
    hiveContext.sql("load data local inpath '/usr/local/soft/spark/examples/src/main/resources/result.txt' into table result");
    val top3df= hiveContext.sql("select ipaddress,category, product,price from (select ipaddress,category, product,price, row_number() OVER (PARTITION BY category ORDER BY  price desc) rank from result ) tmp_result where tmp_result.rank <= 3 ");
    hiveContext.sql("DROP TABLE IF EXISTS top3_result")
    top3df.write.saveAsTable("top3_result")
    top3df.write.saveAsTable("top3_result")
  }

  def window_new(){
    val conf = new SparkConf().setAppName("WindowFunctionOperation")
    val sc = new SparkContext(conf);
    val spark=SparkSession.builder().config(conf).getOrCreate()
    spark.sql("use xtwy");
    spark.sql("DROP TABLE IF EXISTS result");
    spark.sql("create table result(ipaddress string,category string,product string,price int) row format delimited fields terminated by '\t' ");
    spark.sql("load data local inpath '/usr/local/soft/spark/examples/src/main/resources/result.txt' into table result");
    val top3df= spark.sql("select ipaddress,category, product,price from "
      +"("
      +"select ipaddress,category, product,price, row_number() OVER " +
        "(PARTITION BY category ORDER BY  price desc) rank from result ) tmp_result where tmp_result.rank <= 3 "
     )

    spark.sql("DROP TABLE IF EXISTS top3_result")
    top3df.write.saveAsTable("top3_result")
    top3df.write.saveAsTable("top3_result")
  }

}
