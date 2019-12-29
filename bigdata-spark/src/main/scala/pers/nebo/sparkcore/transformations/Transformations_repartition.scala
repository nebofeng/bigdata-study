package pers.nebo.sparkcore.transformations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * repartition
  * 重新分区，可以将RDD的分区增多或者减少，会产生shuffle
  * coalesce(num,true) = repartition(num)
  */
object Transformations_repartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("repartition")
    val sc = new SparkContext(conf)
    val rdd1: RDD[String] = sc.parallelize(List[String](
      "love1", "love2", "love3", "love4",
      "love5", "love6", "love7", "love8",
      "love9", "love10", "love11", "love12"),3)
    val rdd2 :RDD[String] = rdd1.mapPartitionsWithIndex((index,iter)=>{
      val list = ListBuffer[String]()
      iter.foreach(one=>{
        list.append(s"rdd1 partition = 【$index】,value = 【$one】")
      })
      list.iterator
    },true)

//    val rdd3 = rdd2.repartition(4)
    val rdd3 = rdd2.repartition(3)
    val rdd4 = rdd3.mapPartitionsWithIndex((index,iter)=>{
      val arr = ArrayBuffer[String]()
      iter.foreach(one=>{
        arr.append(s"rdd3 partition = 【$index】,value =  【$one】")
      })
      arr.iterator
    })
    val results : Array[String] = rdd4.collect()
    results.foreach(println)
    sc.stop()
  }
}
