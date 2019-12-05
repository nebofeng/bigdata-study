package pers.nebo.sparksql

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/8/22
  * @ des : json操作
  *
  * 1.6  redisterTempTable
  * 2.x createOrReplaceTemplvire或createOrReplaceGolobalTempView
  *
  *


  * 读取json格式的文件创建DataFrame
  *
  * 注意 ：json文件中不能嵌套json格式的内容
  *
  * 1.读取json格式两种方式
  * 2.df.show默认显示前20行，使用df.show(行数)显示多行
  * 3.df.javaRDD/(scala df.rdd) 将DataFrame转换成RDD
  * 4.df.printSchema()显示DataFrame中的Schema信息
  * 5.dataFram自带的API 操作DataFrame
  * 6.想使用sql查询，首先要将DataFrame注册成临时表：df.registerTempTable("jtable")，再使用sql,
  * 怎么使用sql?sqlContext.sql("sql语句")
  * 7.不能读取嵌套的json文件
  * 8.df加载过来之后将列按照ascii排序了
  *
  */
object JsonOperation {

  def main(args: Array[String]): Unit = {

  }

  def JsonOperation_old(){
    /**
      * 1.6.x api
      *
      */

    val conf=new SparkConf().setAppName("JsonOperation")
    val sc=new SparkContext(conf);
    val sqlContext=new SQLContext(sc);
    import sqlContext.implicits._
    val df1=sqlContext.read.json("hdfs://hadoop1:9000/examples/src/main/resources/people.json")
    df1.printSchema()
    df1.registerTempTable("people")
    val teenagers =sqlContext.sql("select name from people where age >=13 and age <= 19");
    teenagers.write.parquet("hdfs://hadoop1:9000/examples/src/main/resources/teenagersresult")
    /**
      * 上面演示的我们通过加载json数据源创建dataframe
      * 那么我们还可以通过并行化的方式 模拟json数据源，生成dataframe
      */
    val list=Array(
      "{\"name\":\"liudehua\",\"age\":51}",
      "{\"name\":\"zhangxueyou\",\"age\":52}",
      "{\"name\":\"guofucheng\",\"age\":53}"
    )
    val jsonRDD= sc.parallelize(list, 1);
    val df2=sqlContext.read.json(jsonRDD);
    df2.printSchema()
    df2.show()
  }


  def JsonOperation_new(){
    /**
      * 2.2.x api
      */

    val conf=new SparkConf().setAppName("JsonOperation")
    val sc=new SparkContext(conf);
    val spark=SparkSession.builder().config(conf)
      .getOrCreate();

    import spark.implicits._
    val df1=spark.read.json("hdfs://hadoop1:9000/examples/src/main/resources/people.json")
    df1.printSchema()
    df1.createOrReplaceTempView("people")
    val teenagers =spark.sql("select name from people where age >=13 and age <= 19");
    teenagers.write.parquet("hdfs://hadoop1:9000/examples/src/main/resources/teenagersresult")
    /**
      * 上面演示的我们通过加载json数据源创建dataframe
      * 那么我们还可以通过并行化的方式 模拟json数据源，生成dataframe
      */
    val list=Array(
      "{\"name\":\"liudehua\",\"age\":51}",
      "{\"name\":\"zhangxueyou\",\"age\":52}",
      "{\"name\":\"guofucheng\",\"age\":53}"
    )
    val jsonRDD= sc.parallelize(list, 1);
    val df2=spark.read.json(jsonRDD);
    df2.printSchema()
    df2.show()
  }
}
