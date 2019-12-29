package pers.nebo.sparkcore.transformations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * join  会产生shuffle
  * （K,V）格式的RDD和（K,V）格式的RDD按照key相同join 得到（K,(V,W)）格式的数据。
  */
object Transformations_join {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("join")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val nameRDD = sc.parallelize(List[(String,String)](("zhangsan","female"),("lisi","male"),("wangwu","female")))
    val scoreRDD = sc.parallelize(List[(String,Int)](("zhangsan",18),("lisi",19),("wangwu",20)))
    val joinRDD: RDD[(String, (String, Int))] = nameRDD.join(scoreRDD)
    joinRDD.foreach(println)
  }
}
