package pers.nebo.sparkcore.aggwordcount

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/8/14
  * @ des : 数据倾斜，先打散，再聚合
  */
object AggWordCountDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val rdd =sc.textFile("")

    rdd.flatMap{ line=> line.split("\t")}
      .map{word=>(word,1)}
      .map{word=>
          {
            val prefix= (new util.Random).nextInt(4)
              (prefix+"_"+word._1,word._2)
          }
      } //这里加上随机数
      .reduceByKey(_+_)
      .map{
        word=>
        {
         val key=word._1.split("_")(1)
        (key,word._2)
        }
      } //这里去掉随机数
      .reduceByKey(_+_)
      .foreach(
        line=>{
          printf(line._1,line._2)
        }
      )

  }


}
