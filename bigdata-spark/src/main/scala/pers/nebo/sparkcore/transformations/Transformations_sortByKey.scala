package pers.nebo.sparkcore.transformations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * sortByKey 默认按照key去排序，作用在K,V格式的RDD上
  */
object Transformations_sortByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("sortByKey")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val infos = sc.parallelize(Array[(String,String)](("f","f"),("a","a"),("c","c"),("b","b")))
    val result = infos.sortByKey(false)
    result.foreach(println)
    sc.stop()
  }
}
