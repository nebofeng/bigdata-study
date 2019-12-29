package pers.nebo.sparkcore.transformations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * zipWithIndex
  * 将RDD和数据下标压缩成一个K,V格式的RDD
  *
  */
object Transformations_zipWithIndex {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("zip")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List[String]("a","b","c"),2)
    val rdd2 = sc.parallelize(List[Int](1,2,3),numSlices = 2)
    val result: RDD[(String, Long)] = rdd1.zipWithIndex()
    result.foreach(print)
  }
}
