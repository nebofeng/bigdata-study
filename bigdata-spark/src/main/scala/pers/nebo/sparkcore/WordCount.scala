package pers.nebo.sparkcore
/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/8/8
  * @ des :持久化
  */


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]): Unit = {
    //sc spark-shell sc.textFile();
    val conf=new SparkConf().setAppName("WordCount")
    val sc=new SparkContext(conf);
    //ctrl + alt  o
    sc.textFile("hdfs://hadoop1:9000/hello.txt")
      .flatMap { line => line.split("\t") }
      .map { word => (word,1) }
      .reduceByKey(_+_)
      .saveAsTextFile("hdfs://hadoop1:9000/result")
  }
}
