package pers.nebo.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/12/1
  * @ des :
  */
object AccumulatorTest {

  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("test").master("local").getOrCreate()
    val sc=spark.sparkContext
    val accumulator:LongAccumulator =sc.longAccumulator
    val rdd1:RDD[String] =sc.textFile("")
    var i=0

    val rdd2=rdd1.map(line=>{
//      i+=1
       accumulator.add(1)
      // 1.6 不能直接使用accumulator.value 获取值，只能打印对象， 2.x 可以
      println(s"Exector accumulator= ${accumulator}")
      line
    })

    rdd2.collect()
    println(s"i= $i")

  }

}
