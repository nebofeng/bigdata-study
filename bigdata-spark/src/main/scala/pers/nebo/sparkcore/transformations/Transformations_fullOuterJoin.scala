package pers.nebo.sparkcore.transformations

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
/**
  * fullOuterJoin
  * (K,V)格式的RDD和(K,W)格式的RDD 使用fullOuterJoin结合是以两边的RDD出现的key为主，得到（K,(Option(V)，Option(W))）
  */
object Transformations_fullOuterJoin {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("fullOuterJoin")
    val sc = new SparkContext(conf)
    val nameRDD = sc.parallelize(List[(String,String)](("zhangsan","female"),("lisi","male"),("wangwu","female"),("maliu","male")),3)
    val scoreRDD = sc.parallelize(List[(String,Int)](("zhangsan",18),("lisi",19),("wangwu",20),("tianqi",21)),4)
    val fullOuterJoin: RDD[(String, (Option[String], Option[Int]))] = nameRDD.fullOuterJoin(scoreRDD)
    fullOuterJoin.foreach(println)
    println("fullOuterJoin RDD partition length = "+fullOuterJoin.getNumPartitions)
    sc.stop()
  }
}
