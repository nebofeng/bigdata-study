package pers.nebo.sparkcore

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/8/12
  * @ des : scala top n demo
  */
object TopN {
  /**
    *  需求： 对一个文件中的单词统计，计算出top3 的
    */

  def topN(): Unit ={
    val conf=new SparkConf().setMaster("local")
                            .setAppName("topN");
    val sc=new SparkContext(conf)
    val filePath="F:\\DATA\\topN.txt"
    val file=sc.textFile(filePath)
    file.flatMap{line=>line.split("\t")}
      .map(line=>(line,1))
      .reduceByKey{_+_}
      .sortBy(_._2,false)
      .take(3)
      .foreach(line=>print(line._1,line._2))


  }

  def topN1(): Unit ={
    val conf=new SparkConf().setMaster("local")
      .setAppName("topN");
    val sc=new SparkContext(conf)
    val filePath="F:\\DATA\\topN.txt"
    val file=sc.textFile(filePath)
    file.flatMap{line=>line.split("\t")}
      .map(line=>(line,1))
      .reduceByKey{_+_}
      .map(line=>(line._2,line._1))
      .sortByKey(false)
      .take(3)
      .foreach(line=>print(line._1,line._2))

  }

  def main(args: Array[String]): Unit = {
   // topN()
    topN1()
  }
}
