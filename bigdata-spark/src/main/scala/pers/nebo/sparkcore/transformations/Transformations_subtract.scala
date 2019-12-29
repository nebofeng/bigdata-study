package pers.nebo.sparkcore.transformations

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * subtract 取RDD的差集
  * subtract两个RDD的类型要一致，结果RDD的分区数与subtract算子前面的RDD的分区个数一致
  */
object Transformations_subtract {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("subtract")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List[String]("zhangsan","lisi","wangwu"),5)
    val rdd2 = sc.parallelize(List[String]("zhangsan","lisi","maliu"),4)
    val subtractRDD: RDD[String] = rdd1.subtract(rdd2)
    subtractRDD.foreach(println)
    println("subtractRDD partition length = "+subtractRDD.getNumPartitions)
    sc.stop()
  }
}
