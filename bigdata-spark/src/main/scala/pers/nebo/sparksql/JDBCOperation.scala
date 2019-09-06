package pers.nebo.sparksql

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/8/22
  * @ des : JDBC操作
  */
object JDBCOperation {
  def main(args: Array[String]): Unit = {

  }

  def JDBCOperation_old(){
    val conf=new SparkConf().setAppName("JDBCOperation")
    val sc=new SparkContext(conf)
    val sqlContext=new SQLContext(sc);
    val properties=new Properties();
    properties.put("user", "root")
    properties.put("password", "")
    val url = "jdbc:mysql://hadoop1:3306/xtwy"
    val stduentDF=sqlContext.read.jdbc(url, "student", properties)
    stduentDF.show();

  }


  def JDBCOperation_new(){
    val conf=new SparkConf().setAppName("JDBCOperation")
    val spark=SparkSession.builder().config(conf).getOrCreate();

    val properties=new Properties();
    properties.put("user", "root")
    properties.put("password", "")
    val url = "jdbc:mysql://hadoop1:3306/xtwy"
    val stduentDF=spark.read.jdbc(url, "student", properties)
    stduentDF.show();

  }




}
