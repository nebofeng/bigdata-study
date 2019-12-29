package pers.nebo.sparkcore.transformations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * aggregateByKey
  * 首先是给定RDD的每个分区一个初始值，然后RDD中每个分区中按照相同的key，结合初始值去合并，最后RDD之间相同的key 聚合。
  */
object Transformations_AggregateByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("aggregateByKey").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd1 = sc.makeRDD(List[(String,Int)](
      ("zhangsan",10),("zhangsan",20),("wangwu",30),
      ("lisi",40),("zhangsan",50),("lisi",60),
      ("wangwu",70),("wangwu",80),("lisi",90)
    ),3)
    rdd1.mapPartitionsWithIndex((index,iter)=>{
        val arr = ArrayBuffer[(String,Int)]()
        iter.foreach(tp=>{
          arr.append(tp)
          println("rdd1 partition index = "+index+",value = "+tp)
        })
       arr.iterator
    }).count()

    /**
      *  0号分区：
      *     ("zhangsan",10) ("zhangsan",20) ("wangwu",30)
      *  1号分区：
      *     ("lisi",40) ("zhangsan",50) ("lisi",60)
      *  2号分区：
      *     ("wangwu",70) ("wangwu",80) ("lisi",90)
      *
      *  经过第二个参数，map端聚合 :
      *   0:("zhangsan",hello~10~20),("wangwu",hello~30)
      *   1:("zhangsan",hello~50),("lisi"，hello~40~60)
      *   2:("lisi",hello~90)，("wangwu",hello~70~80)
      *
      *   分区合并后：("zhangsan",hello~10~20#hello~50),("lisi",hello~40~60#hello~90),("wangwu",hello~30#hello~70~80)
      *   ("zhangsan")
      */
    val result: RDD[(String, String)] = rdd1.aggregateByKey("hello")((s, v)=>{s+"~"+v}, (s1, s2)=>{s1+"#"+s2})
    result.foreach(print)
  }
}
