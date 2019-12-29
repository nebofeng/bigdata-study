package pers.nebo.sparkcore.transformations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * leftOuterJoin
  * (K,V)格式的RDD和(K,W)格式的RDD 使用leftOuterJoin结合是以左边的RDD出现的key为主，得到（K,(V,Option(W))）
  */
object Transformations_leftOuterJoin {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("leftOuterJoin")
    val sc = new SparkContext(conf)
    val nameRDD = sc.parallelize(List[(String,String)](("zhangsan","female"),("lisi","male"),("wangwu","female"),("maliu","male")))
    val scoreRDD = sc.parallelize(List[(String,Int)](("zhangsan",18),("lisi",19),("wangwu",20),("tianqi",21)))
    val leftOuterJoin: RDD[(String, (String, Option[Int]))] = nameRDD.leftOuterJoin(scoreRDD)
    leftOuterJoin.foreach(println)
    sc.stop()
  }
}
