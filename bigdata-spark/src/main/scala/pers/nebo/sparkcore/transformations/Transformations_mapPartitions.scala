package pers.nebo.sparkcore.transformations

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * mapPartitions 遍历的是每个分区中的数据，一个个分区的遍历
  * 相对于map 一条条处理数据，性能比较高。
  */
object Transformations_mapPartitions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("mapPartitions")
    val sc = new SparkContext(conf)
    val infos = sc.parallelize(List[String]("a","b","c","d","e","f","g"),4)
    val result = infos.mapPartitions(iter=>{
      println("创建数据库连接... ... ")
      val array = ArrayBuffer[String]()
      while(iter.hasNext){
        val s = iter.next()
        println("拼接sql... ... "+s)
        array.append(s)
      }
      println("关闭数据库连接... ... ")
      array.iterator
    })
    result.count()
    sc.stop()
  }
}
