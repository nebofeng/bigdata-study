package pers.nebo.sparkcore.transformations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * groupBy 按照指定的规则，将数据分组
  */
object Transformations_groupBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("groupBy")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List[(String,Double)](("zhangsan",66.5),("lisi",33.2),("zhangsan",66.7),("lisi",33.4),("zhangsan",66.8),("wangwu",29.8)))
    val result: RDD[(Boolean, Iterable[(String, Double)])] = rdd.groupBy(one => {
      one._2 > 34
    })
    result.foreach(print)


//    val rdd1: RDD[String] = sc.parallelize(List[String](
//      "love1", "love2", "love3", "love4",
//      "love5", "love6", "love7", "love8",
//      "love9", "love10", "love11", "love12"),3)
//
//    val result: RDD[(String, Iterable[String])] = rdd1.groupBy(one=>{one.split("")(4)})
//    result.foreach(print)


  }
}
