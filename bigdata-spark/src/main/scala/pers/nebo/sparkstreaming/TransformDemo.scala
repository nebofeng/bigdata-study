package pers.nebo.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/8/30
  * @ des : TransformDemo
  */
object TransformDemo {

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

    val fillter=ssc.sparkContext.parallelize(List(",","?","!",".")).map { param => (param,true) }
    ssc.checkpoint(".")
    val fileDS=ssc.socketTextStream("hadoop1", 9999)
    val wordcount=fileDS.flatMap { line => line.split("\t") }
      .map { word => (word,1) }

    val needwordDS=wordcount.transform(line=>{
         val leftRdd =line.leftOuterJoin(fillter)
         val needWord= leftRdd.filter(tuple=>{
           if(tuple._2._2.isEmpty) {
              true
           }else{
             false
           }
         })
      needWord.map(tuple=>(tuple._1,1))
    })


    val wcDS= needwordDS.reduceByKey(_+_);
    wcDS.print();
    ssc.start();
    ssc.awaitTermination();

  }

}
