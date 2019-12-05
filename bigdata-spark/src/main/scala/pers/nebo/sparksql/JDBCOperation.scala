package pers.nebo.sparksql

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext, SaveMode, SparkSession}

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


   //第二种方式

    val map =Map[String,String](
      "url"->"",
       "driver"->"",
       "user"->"",
       "passwd"->"",
    "dbtable"->""//表名

    )

    val score:DataFrame = spark.read.format("jdbc").options(map).load()

    // 第三种方式

    val reader: DataFrameReader =spark.read.format("jdbc")
      .option("url","")
      .option("dirver","")
      .option("user","")
      .option("password","123456")
      .option("dbtable","score")
    val score2:DataFrame =reader.load()
    score2.show()

     //将上马两张表注册成为临时表 做关联查询

    stduentDF.createOrReplaceTempView("student")
    score.createOrReplaceTempView("score")
    spark.sql("").show()


   //将结果保存再 mysql 中 ,String 格式默认为 text ，如果不使用这个格式 ，可以自己建表创建各个列的格式再保存

    val result:DataFrame=spark.sql("")
    result.write.mode(SaveMode.Append).jdbc("","",properties)


    //第四种方法

    spark.read.jdbc("jdbc:","(sql )t ",properties)


  }




}
