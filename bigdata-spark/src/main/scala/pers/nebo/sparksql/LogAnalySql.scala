package pers.nebo.sparksql

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/8/28
  * @ des :
  */
object LogAnalySql {

  case class ApacheAccesslog(
                              ipAddress:String, // ip地址
                              clientIndentd:String, //标识符
                              userId:String ,//用户ID
                              dateTime:String ,//时间
                              method:String ,//请求方式
                              endPoint:String ,//目标地址
                              protocol:String ,//协议
                              responseCode:Int ,//网页请求响应类型
                              contentSize:Long   //内容长度

                            )

  object ApacheAccesslog {
    def parseLog(log:String):ApacheAccesslog={
      val logArray= log.split("#");
      val url=logArray(4).split(" ")

      ApacheAccesslog(logArray(0),logArray(1),logArray(2),logArray(3),url(0),url(1),url(2),logArray(5).toInt,logArray(6).toLong);
    }

  }


  def main(args: Array[String]): Unit = {

  }


  def old_api(){
    val sparkconf=new SparkConf().setAppName("LogAnalySql")
    val sc=new SparkContext(sparkconf);
    val sqlContext=new SQLContext(sc);
    import sqlContext.implicits._

    val logsDF= sc.textFile("hdfs://hadoop1:9000/apache/")
      .map { log => ApacheAccesslog.parseLog(log) }
      .toDF()
    logsDF.registerTempTable("log");
    sqlContext.cacheTable("log")
    /**
      * 需求一：The average, min, and max content size of responses returned from the server.
      */

    sqlContext.sql("select avg(contentSize),min(contentSize),max(contentSize) from log").show()
    /**
      * 需求二A count of response code's returned.
      */

    val sql="""
      select responseCode,count(*)
      from
        log
       group by
       responseCode
       limit 10
      """
    sqlContext.sql(sql).show()
    /**
      * 需求三All IPAddresses that have accessed this server more than N times.
      */
    val ipSql="""
      select ipAddress,count(1) as total
      from
        log
        group by
        ipAddress
        having
         total > 10
      """
    sqlContext.sql(ipSql).show()
    /**
      * 需求四The top endpoints requested by count.  TopN
      */
    val topSql="""
      select endPoint,count(1) as total
      from
      log
      group by
      endPoint
      order by
      total desc
      limit 3
      """
    sqlContext.sql(topSql).show()
  }

  def new_api(){
    val conf = new SparkConf()
    val sc=new SparkContext(conf)
    val spark=SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val logDF= sc.textFile("").map{
        line=>{
          ApacheAccesslog.parseLog(line)
        }
    }.toDF();

    logDF.createOrReplaceTempView("log")

    spark.catalog.cacheTable("log")
    /**
      * 需求一：The average, min, and max content size of responses returned from the server.
      */
    spark.sql("select avg(contentSize),min(contentSize),max(contentSize) from log").show()

    /**
      * 需求二A count of response code's returned.
      */
    spark.sql("select responsecode,count(*) from log group by responsecode")

    /**
      * 需求三All IPAddresses that have accessed this server more than N times.
      */
    spark.sql("select ip ,count(*) as times  from  log group by ip  having times>N").show()

    /**
      * 需求四The top endpoints requested by count.  TopN
      */

    spark.sql("select endpoints,count(*) as count from log group by endpoints order by count desc limit N")

  }
}
