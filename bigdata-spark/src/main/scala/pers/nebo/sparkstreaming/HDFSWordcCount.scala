package pers.nebo.sparkstreaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/8/29
  * @ des :spark streaming wordcount
  */
object HDFSWordcCount {
  def main(args: Array[String]): Unit = {

    val conf=new SparkConf().setMaster("local[2]").setAppName("HDFSWordcCount")
    val ssc=new StreamingContext(conf,Seconds(2));

    val fileDS=ssc.textFileStream("hdfs://hadoop1:9000/hdfs")
    val wordcountds=fileDS.flatMap { line => line.split("\t") }
      .map { word => (word,1) }
      .reduceByKey(_+_)
    wordcountds.print()
    ssc.start()
    ssc.awaitTermination();
  }

}
