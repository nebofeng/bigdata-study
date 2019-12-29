package pers.nebo.sparkcore.transformations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * union 合并两个RDD
  * union 合并RDD ，两个RDD必须是同种类型，不必要是K,V格式的RDD
  */
object Transformations_union {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("union")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List[String]("zhangsan","lisi","wangwu","maliu"),3)
    val rdd2 = sc.parallelize(List[String]("a","b","c","d"),4)
    val unionRDD: RDD[String] = rdd1.union(rdd2)
    unionRDD.foreach(println)
    println("unionRDD partitioin length = "+unionRDD.getNumPartitions)
    sc.stop()
  }
}
