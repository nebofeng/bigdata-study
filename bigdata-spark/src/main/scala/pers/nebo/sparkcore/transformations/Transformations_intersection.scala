package pers.nebo.sparkcore.transformations

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * intersection 取两个RDD的交集，两个RDD的类型要一致
  * 结果RDD的分区数与父rdd多的一致
  */
object Transformations_intersection {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("intersection")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List[String]("zhangsan","lisi","wangwu"),5)
    val rdd2 = sc.parallelize(List[String]("zhangsan","lisi","maliu"),4)
    val intersectionRDD: RDD[String] = rdd1.intersection(rdd2)
    intersectionRDD.foreach(println)
    println("intersectionRDD partition length = "+intersectionRDD.getNumPartitions)
    sc.stop()
  }
}
