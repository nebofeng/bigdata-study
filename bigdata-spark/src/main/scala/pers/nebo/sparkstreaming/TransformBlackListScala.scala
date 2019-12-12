package pers.nebo.sparkstreaming

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, sql}

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/12/12
  * @ des : 黑名单过滤
  */
object TransformBlackListScala {
  def main(args: Array[String]): Unit = {
    /**
      * sc 也可以由spark session创建出来
      */
    val conf=new SparkConf()
    conf.setAppName("balcklisttest")
    conf.setMaster("local[2]")
    val sc=new SparkContext(conf)

    val ssc = new StreamingContext(sc,Seconds(5))

    val blackList:List[String]=List[String]("name1","name2")


    val blackBroadcat = ssc.sparkContext.broadcast(blackList)


    /**
      * 从实时数据中发现数据的第二位是黑名单 ，过滤掉
      */
    val ds = ssc.textFileStream("/").map(line=>{


      (line.split("\t")(1),line)


    })

    //少量的数据使用遍历
    val result:DStream[String] = ds.transform(pairRDD=>{

         val filterRdd:RDD[(String,String)]=pairRDD.filter(line=>{
           val nameList=blackBroadcat.value
           !nameList.contains(line._1)

         })

          filterRdd.map(line=>{line._2})
    })


    result.print()




   /**
    *从实时数据中发现数据的第二位是黑名单 ，过滤掉
    */
    val ds1 = ssc.textFileStream("/").map(line=>{

      (line,line.split("\t")(1))

    })

    /**
      * 数据量大的话，使用rdd, 格式为 名字 ，true
      *
      * 使用rdd  leftoutjoin来处理
      */

























  }
}
