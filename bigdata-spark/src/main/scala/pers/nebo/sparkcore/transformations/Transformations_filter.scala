package pers.nebo.sparkcore.transformations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * filter 过滤算子
  * 过滤数据，返回true的数据会被留下
  */
object Transformations_filter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("filter")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val infos = sc.makeRDD(List[Int](1,2,3,4,5))
    val result = infos.filter(one=>{
      one>3
    })
    result.foreach(println)
    sc.stop()
  }

}
