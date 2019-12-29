package pers.nebo.sparkcore.transformations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * sortBy 排序，参数中指定按照什么规则去排序，第二个参数 true/false 指定升序或者降序
  * 无需作用在K,V格式的RDD上
  */
object Transformations_sortBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("sortBy")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val infos = sc.parallelize(Array[(String,String)](("f","f"),("a","a"),("c","c"),("b","b")))
    val result = infos.sortBy(tp=>{
      tp._1
    },false)
    result.foreach(println)

    val infos1 = sc.parallelize(Array[Int](400,200,500,100,300))
    val result1 = infos1.sortBy(one=>{one/100},false)
    result1.foreach(println)
    sc.stop()
  }
}
