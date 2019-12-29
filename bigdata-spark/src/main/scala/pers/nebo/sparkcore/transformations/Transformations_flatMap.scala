package pers.nebo.sparkcore.transformations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * flatMap 是一对多的关系
  * 处理一条数据得到多条数据结果
  */
object Transformations_flatMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("map").setMaster("local")
    val sc = new SparkContext(conf)
    val infos = sc.parallelize(Array[String]("hello spark","hello hdfs","hello bjsxt"))
    val result = infos.flatMap(one=>{
      one.split(" ")
    })
    result.foreach(println)
  }
}
