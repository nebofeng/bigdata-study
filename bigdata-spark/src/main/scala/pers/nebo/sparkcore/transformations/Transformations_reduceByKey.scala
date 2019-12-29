package pers.nebo.sparkcore.transformations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * reduceByKey
  * 首先会根据key 去分组，然后处理每个组，将每个组内的value聚合
  * 作用在K,V格式的RDD上
  */
object Transformations_reduceByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("reduceByKey")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val infos = sc.parallelize(List[(String,Int)](("zhangsan",1),("zhangsan",2),("zhangsan",3),("lisi",100),("lisi",200)))
    val result = infos.reduceByKey((v1,v2)=>{v1+v2})
    result.foreach(println)
    sc.stop()
  }
}
