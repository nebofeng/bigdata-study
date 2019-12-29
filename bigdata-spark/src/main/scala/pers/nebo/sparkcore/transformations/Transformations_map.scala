package pers.nebo.sparkcore.transformations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * map 处理数据是一对一的关系
  * 进入一条数据处理，出来的还是一条数据
  */
object Transformations_map {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("map").setMaster("local")
    val sc = new SparkContext(conf)
    val infos = sc.parallelize(Array[String]("hello spark","hello hdfs","hello bjsxt"))
    val result = infos.map(one=>{one.split(" ")})
    result.foreach(arr=>{arr.foreach(println)})
    sc.stop()
  }
}
