package pers.nebo.sparkcore.transformations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * zip
  * 将两个RDD 合成一个K,V格式的RDD,分区数要相同，每个分区中的元素必须相同
  */
object Transformations_zip {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("zip")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List[String]("a","b","c"),2)
    val rdd2 = sc.parallelize(List[Int](1,2,3),numSlices = 2)

    val result: RDD[(String, Int)] = rdd1.zip(rdd2)
    result.foreach(print)
  }
}
