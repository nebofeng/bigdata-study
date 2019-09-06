package pers.nebo.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/8/2
  * @ des :
  */
object WordCountLocal {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("WCLocal")
    val sc = new SparkContext(conf)
    var localData="";
    sc.textFile(localData)
       .flatMap{line=>line.split("\t")}
       .map{line=>(line,1)}
      .reduceByKey(_+_)
      .foreach( result=>{
               print (result)
             })

  }

}
