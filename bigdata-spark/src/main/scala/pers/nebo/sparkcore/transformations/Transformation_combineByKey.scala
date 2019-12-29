package pers.nebo.sparkcore.transformations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * combineByKey
  * 首先给RDD中每个分区中的每个key一个初始值
  * 其次在RDD每个分区内部 相同的key聚合一次
  * 再次在RDD不同的分区之间将相同的key结果聚合一次
  */
object Transformation_combineByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("combineByKey")
    val sc = new SparkContext(conf)
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List[(String, Int)](
      ("zhangsan", 10), ("zhangsan", 20), ("wangwu", 30),
      ("lisi", 40), ("zhangsan", 50), ("lisi", 60),
      ("wangwu", 70), ("wangwu", 80), ("lisi", 90)
    ), 3)
    rdd1.mapPartitionsWithIndex((index,iter)=>{
      val arr = ArrayBuffer[(String,Int)]()
      iter.foreach(tp=>{
        arr.append(tp)
        println("rdd1 partition index = "+index+",value = "+tp)
      })
      arr.iterator
    }).count()
    println("++++++++++++++++++++++++++++++++++++++++++++")
    /**
      * 0号分区：("zhangsan", 10), ("zhangsan", 20), ("wangwu", 30)
      * 1号分区：("lisi", 40), ("zhangsan", 50), ("lisi", 60)
      * 2号分区：("wangwu", 70), ("wangwu", 80), ("lisi", 90)
      *
      * 初始化后：
      * 0号分区：("zhangsan", 10hello),("wangwu", 30hello)
      * 1号分区：("lisi", 40hello), ("zhangsan", 50hello)
      * 2号分区：("wangwu", 70hello),("lisi", 90hello)
      *
      * 经过RDD分区内的合并后:
      * 0号分区：("zhangsan", 10hello@20)，("wangwu", 30hello)
      * 1号分区：("lisi", 40hello@60), ("zhangsan", 50hello)
      * 2号分区：("wangwu", 70hello@80),("lisi", 90hello)
      *
      * 经过RDD分区之间的合并：("zhangsan", 10hello@20#50hello),("lisi",40hello@60#90hello),("wangwu", 30hello#70hello@80)
      */
    //    rdd1.combineByKey((v:Int)=>{v+"hello"},(s:String,v:Int)=>{s+"@"+v},(s1:String,s2:String)=>{s1+"#"+s2})
    val result: RDD[(String, String)] = rdd1.combineByKey(v=>{v+"hello"}, (s:String, v)=>{s+"@"+v}, (s1:String, s2:String)=>{s1+"#"+s2})
    result.foreach(println)
  }
}
