package pers.nebo.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/8/23
  * @ des :
  */
object UDFDemo {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("UDFDemo")
    val sc=new SparkContext(conf);
    val hiveContext=new HiveContext(sc);
    //sqlContext =new SQLContext(sc);
    /**
      * 第一个参数：自定函数的函数名
      * 第二个参数：就是一个匿名函数
      */
    hiveContext.udf.register("strLen", (str:String)=>{
      if(str != null){
        str.length()
      }else{
        0
      }
    })
    hiveContext.sql("select strLen(category) from xtwy.result").show()

    /**
      * spark 2.2.2
      */

    val spark=SparkSession.builder().config(conf).getOrCreate()

    spark.udf.register("strLen", (str:String)=>{
      if(str != null){
        str.length()
      }else{
        0
      }
    })
  }

}
