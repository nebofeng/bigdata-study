package pers.nebo.sparkcore.logdemo

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/8/12
  * @ des : 日志统计demo
  *
  */
object LogDemo {

  def main(args: Array[String]): Unit = {

    val filepath=this.getClass.getResource("/sparkcore/log.txt").getPath

    val conf=new SparkConf().setMaster("local").setAppName("LogAnalyer")
    val sc=new SparkContext(conf);
    val logsRDD=sc.textFile(filepath)
      .map { line => ApacheAccesslog.parseLog(line) }
      .cache()
    /**
      * 需求一
      * The average, min, and max content size of responses returned from the server.
      */
    val contextSize=logsRDD.map { log => log.contenSize }
    // get max
    val maxSize=contextSize.max()
    //get min
    val minSize=contextSize.min()
    // total / count
    //get average
    val averageSize=contextSize.reduce(_+_)/contextSize.count()
    println("=============================需求一-==============================");
    println("最大值："+maxSize  + "  最小值："+minSize + "   平均值："+averageSize);
    /**
      * 需求二
      * A count of response code's returned.
      */
    println("=============================需求二-==============================");
    logsRDD.map { log => (log.responseCode,1) }
      .reduceByKey(_+_)
      .foreach(result => println(" 响应状态："+result._1 + "  出现的次数："+result._2))
    /**
      * 需求三
      * All IPAddresses that have accessed this server more than N times.
      */
    println("=============================需求三-==============================");
    val result= logsRDD.map { log =>( log.ipAddress,1) }
      .reduceByKey(_+_)
      .filter(result => result._2 > 1)   // > 10000
      .take(2)  //  >  10
    for( tuple <- result){
      println("ip : "+tuple._1 + "  出现的次数："+tuple._2);
    }
    /**
      * 需求四
      * The top endpoints requested by count.  TopN
      */
    println("=============================需求四-==============================");
    val topN=logsRDD.map { log => (log.endPoint,1) }
      .reduceByKey(_+_)
      .map(result => (result._2,result._1))
      .sortByKey(false)
      .take(2)
    for(tuple <- topN){
      println("目标地址 : "+tuple._2 + "  出现的次数："+tuple._1);
    }
    logsRDD.unpersist(true)
  }

}
