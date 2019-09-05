package pers.nebo.sparksql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/9/1
  * @ des : spark 2.x dataset 操作
  */
object DataSetOperation {


  /**
    *
    * 大家通过观察刚刚的Dataset APi发现，DataSet的api跟RDD的非常类型。
    * DataSet的APi 也分transformation操作和Action的操作。
    * transformation 具有lazy的特性
    * action会触发job的运行
    *
    * Dataset里面也有持久化的操作。RDD的时候也有持久化。
    *
    * 包括我们之前在第二阶段，重点讲解了RDD调优，而那些调优的思路，对这个DataSet一样通用！！
    *
    * DataSet【Untyped transformations】
    *
    *
    * 统计一下，每个班的应届毕业生按性别统计平均年纪
    * 思路：
    * 1） 按班级和性别分组
    * 2）统计平均年纪
    *
    */


  def main(args: Array[String]): Unit = {
      val conf= new SparkConf()
      val sc=new SparkContext(conf)
      val spark=SparkSession.builder().config(conf).getOrCreate()

    //导入隐式转换
    import spark.implicits._
    import org.apache.spark.sql.functions._

      val studentDF=spark.read.json("C:\\Users\\Administrator.USER-20160518JB\\Desktop\\student.json")
      val classDF=spark.read.json("C:\\Users\\Administrator.USER-20160518JB\\Desktop\\class.json")

      studentDF.filter("isnew != 'no'")
        .join(classDF,$"classID" === $"id")
        .groupBy(classDF("classname"),studentDF("gender") )
        .agg(avg(studentDF("age")))
        .show()

  }
}
