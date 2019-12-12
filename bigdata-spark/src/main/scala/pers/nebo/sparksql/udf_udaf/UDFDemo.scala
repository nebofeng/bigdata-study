package pers.nebo.sparksql.udf_udaf

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

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


    val nameList:List[String] =List[String]("demo1","demo2","demo3")
    import spark.implicits._
    val nameDF: DataFrame = nameList.toDF("name")
    nameDF.createOrReplaceTempView("students")
    spark.udf.register("STRLEN",(n:String)=>{
           n.length

    })

    spark.sql("select name ,STREN(name) as length from students sort by length desc ").show(100)
  }

}
