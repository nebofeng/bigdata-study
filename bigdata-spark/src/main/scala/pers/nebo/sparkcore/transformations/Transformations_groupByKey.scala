package pers.nebo.sparkcore.transformations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * groupByKey
  * 根据key 去将相同的key 对应的value合并在一起
  * （K,V）=>(K,[V])
  */
object Transformations_groupByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("groupByKey")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List[(String,Double)](("zhangsan",66.5),("lisi",33.2),("zhangsan",66.7),("lisi",33.4),("zhangsan",66.8),("wangwu",29.8)))
    val rdd1 = rdd.groupByKey()
    rdd1.foreach(info=>{
      val name = info._1
      val value: Iterable[Double] = info._2
      val list: List[Double] = info._2.toList
      print("name = "+name+",list = "+list)
    })
    sc.stop()
  }

}
