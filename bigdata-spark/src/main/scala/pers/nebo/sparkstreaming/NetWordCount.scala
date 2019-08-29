package pers.nebo.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/8/29
  * @ des :
  */
object NetWordCount {
  def main(args: Array[String]): Unit = {
    /**
      * local[1]  中括号里面的数字都代表的是启动几个工作线程
      * 默认情况下是一个工作线程。那么做为sparkstreaming 我们至少要开启
      * 两个线程，因为其中一个线程用来接收数据，这样另外一个线程用来处理数据。
      */
    val conf=new SparkConf().setMaster("local[2]").setAppName("NetWordCount")
    /**
      * Seconds  指的是每次数据数据的时间范围 （bacth interval）
      */
    val  ssc=new StreamingContext(conf,Seconds(2));

    val fileDS=ssc.socketTextStream("hadoop1", 9999)
    val wordcount=fileDS.flatMap { line => line.split("\t") }
      .map { word => (word,1) }
      .reduceByKey(_+_)
    /**
      * 打印RDD里面前十个元素
      */
    wordcount.print()
    //启动应用
    ssc.start()
    //等待任务结束
    ssc.awaitTermination()

  }
}
