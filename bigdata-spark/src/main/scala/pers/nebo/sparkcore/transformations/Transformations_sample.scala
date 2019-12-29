package pers.nebo.sparkcore.transformations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * sample随机抽样，参数sample(有无放回抽样，抽样的比例，种子)
  * 有种子和无种子的区别：
  *   有种子是只要针对数据源一样，都是指定相同的参数，那么每次抽样到的数据都是一样的
  *   没有种子是针对同一个数据源，每次抽样都是随机抽样
  */
object Transformations_sample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("sample")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("./data/sampleData.txt")
    val result = lines.sample(true,0.01,100)
    result.foreach(println)
    sc.stop()
  }
}
