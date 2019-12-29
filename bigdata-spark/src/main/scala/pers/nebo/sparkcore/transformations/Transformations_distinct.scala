package pers.nebo.sparkcore.transformations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * distinct 去重，有shuffle产生，内部实际是 map+reduceByKey+map实现
  */
object Transformations_distinct {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("distinct")
    val sc = new SparkContext(conf)
    val infos = sc.parallelize(List[String]("a","a","b","b","c","c","d"),4)
    val result: RDD[String] = infos.distinct()
    result.foreach(println)
    sc.stop()
  }
}
