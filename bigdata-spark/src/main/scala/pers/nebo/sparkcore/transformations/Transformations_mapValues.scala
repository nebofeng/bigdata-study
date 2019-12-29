package pers.nebo.sparkcore.transformations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * mapValues
  * 针对K,V格式的数据，只对Value做操作，Key保持不变
  */
object Transformations_mapValues {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("mapValues")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val infos: RDD[(String, String)] = sc.makeRDD(List[(String, String)](("zhangsna", "18"), ("lisi", "20"), ("wangwu", "30")))
    val result: RDD[(String, String)] = infos.mapValues(s => {
      s + " " + "zhangsan18"
    })
    result.foreach(print)

    sc.stop()
  }
}
