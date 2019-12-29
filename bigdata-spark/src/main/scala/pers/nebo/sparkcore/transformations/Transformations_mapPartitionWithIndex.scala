package pers.nebo.sparkcore.transformations

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * mapPartitionsWithIndex
  * 可以拿到每个RDD中的分区，以及分区中的数据
  */
object Transformations_mapPartitionWithIndex {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("mapPartitionWithIndex")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("./data/words",5)
    val result = lines.mapPartitionsWithIndex((index,iter)=>{
      val arr = ArrayBuffer[String]()
      iter.foreach(one=>{
        arr.append(s"partition = 【$index】,value = $one")
      })
      arr.iterator
    },true)
    result.foreach(println)
    sc.stop()
  }
}
