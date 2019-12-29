package pers.nebo.sparkcore.transformations

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * flatMapValues
  * (K,V) -> (K,V)
  * 作用在K,V格式的RDD上，对一个Key的一个Value返回多个Value
  */
object Transformations_flatMapValues {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("flatMapValues")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val infos: RDD[(String, String)] = sc.makeRDD(List[(String, String)](("zhangsna", "18"), ("lisi", "20"), ("wangwu", "30")))
    val transInfo: RDD[(String, String)] = infos.mapValues(s => {
      s + " " + "zhangsan18"
    })
    val result = transInfo.flatMapValues(s=>{
      s.split(" ")
    })
    result.foreach(print)

    sc.stop()
  }
}
