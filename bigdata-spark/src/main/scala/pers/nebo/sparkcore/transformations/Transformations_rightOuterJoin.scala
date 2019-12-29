package pers.nebo.sparkcore.transformations

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
/**
  * rightOuterJoin
  * (K,V)格式的RDD和(K,W)格式的RDD 使用rightOuterJoin结合是以右边的RDD出现的key为主，得到（K,(Option(V)，W)）
  */
object Transformations_rightOuterJoin {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("rightOuterJoin")
    val sc = new SparkContext(conf)
    val nameRDD = sc.parallelize(List[(String,String)](("zhangsan","female"),("lisi","male"),("wangwu","female"),("maliu","male")),3)
    val scoreRDD = sc.parallelize(List[(String,Int)](("zhangsan",18),("lisi",19),("wangwu",20),("tianqi",21)),4)
    val rightOuterJoin: RDD[(String, (Option[String], Int))] = nameRDD.rightOuterJoin(scoreRDD)
    rightOuterJoin.foreach(println)
    println("rightOuterJoin RDD partition length = "+rightOuterJoin.getNumPartitions)
    sc.stop()
  }
}
