package pers.nebo.sparkcore.transformations

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Transformations_cogroup {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("cogroup")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List[(String,String)](("zhangsan","female"),("zhangsan","female1"),("lisi","male"),("wangwu","female"),("maliu","male")),3)
    val rdd2 = sc.parallelize(List[(String,Int)](("zhangsan",18),("lisi",19),("lisi",190),("wangwu",20),("tianqi",21)),4)
    val resultRDD: RDD[(String, (Iterable[String], Iterable[Int]))] = rdd1.cogroup(rdd2)
    resultRDD.foreach(info=>{
      val key = info._1
      val value1: List[String] = info._2._1.toList
      val value2: List[Int] = info._2._2.toList
      println("key = "+key+",value1 = "+value1+",value2 = "+value2)
    })
    println("resultRDD partitioin length = "+resultRDD.getNumPartitions)
    sc.stop()
  }
}
