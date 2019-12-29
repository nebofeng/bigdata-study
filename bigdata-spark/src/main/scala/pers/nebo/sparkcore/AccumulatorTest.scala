package pers.nebo.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/12/1
  * @ des : 累加器的使用
  *
  *
  *（1） 如果没触发到action ，则lazy 不会触发，累加器的值也不会发生变化。
  *（2）一个action 操作，会触发一次累加器，所以如果多个action操作的话，中间使用cache（） ，
  * 这样下一个aciton会使用cache的内容，则不会多计算累加器
  */
object AccumulatorTest {

  def main(args: Array[String]): Unit = {

    //累加器的使用2.3.3版本

    val spark=SparkSession.builder().appName("test").master("local").getOrCreate()
    val sc=spark.sparkContext
    val accumulator:LongAccumulator =sc.longAccumulator
    val rdd1:RDD[String] =sc.textFile("/DATA/topN.txt")
    var i=0

    val rdd2=rdd1.map(line=>{
//      i+=1
       accumulator.add(-1)

      // 1.6 不能直接使用accumulator.value 获取值，只能打印对象， 2.x 可以
      println(s"Exector accumulator= ${accumulator}")
      line
    })

    rdd2.collect()
    println(s"i= ${accumulator.value}")


    val accumulator2:LongAccumulator =sc.longAccumulator

    val rdd3:RDD[String] =sc.textFile("/DATA/topN.txt",2)
    println("rdd3  partition length==="+rdd3.getNumPartitions)
    val rdd4=rdd3.map(line=>{
      //      i+=1
      accumulator2.add(1)
      // 1.6 不能直接使用accumulator.value 获取值，只能打印对象， 2.x 可以
      println(s"Exector accumulator= ${accumulator}")
      line
    })

    //使用cache 这样下一次就不会再触发累加器了。注释掉下面这行累加器则会翻倍。
     rdd4.cache()

    // 这里的sortby 没有任何意思只是为了使用sort 函数
    val rdd5= rdd4.sortBy(line=>{})
    // sort by key 当你的分区大于1的时候，它会有一个抽样 ，触发rdd的action
    rdd5.collect()

    println(s"i= ${accumulator2.value}")






  }

}
