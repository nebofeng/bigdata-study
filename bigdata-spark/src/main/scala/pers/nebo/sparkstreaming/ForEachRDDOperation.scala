package pers.nebo.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/8/30
  * @ des :
  */
object ForEachRDDOperation {
  def main(args: Array[String]): Unit = {

    val conf=new SparkConf().setMaster("local[2]").setAppName("ForEachRDDOperation")

    val  ssc=new StreamingContext(conf,Seconds(2));

    val fileDS=ssc.socketTextStream("192.168.32.110", 9999);
    val wordcountDS=fileDS.flatMap { line => line.split("\t") }
      .map { word => (word,1) }
      .reduceByKey(_+_)

    /**
      * foreach rdd
      * 1. 一定要有action 不然不执行
      * 2. 内部拿到的rdd的transformation 外部 是在driver端执行的，可以利用这个特点改变广播变量
      * （从外部读取）
      */
    wordcountDS.foreachRDD( partitionOfRecords =>{

          // driver  端 run
      partitionOfRecords.foreachPartition( records =>{


        val connect= MysqlPool.getJdbcCoon();
        while(records.hasNext){
          val tuple=  records.next();
          val sql="insert into wordcount values( now(),'"+tuple._1+"', "+tuple._2.toInt+")";
          val statement=connect.createStatement();
          statement.executeUpdate(sql);
          print(sql);
        }
        MysqlPool.releaseConn(connect)

      })

    })

    ssc.start()

    ssc.awaitTermination()

  }
}
