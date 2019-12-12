package pers.nebo.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/8/29
  * @ des :
  */
object NetWordCountUpdateStateByKey {
  def main(args: Array[String]): Unit = {

    /**
      * local[1]  中括号里面的数字都代表的是启动几个工作线程
      * 默认情况下是一个工作线程。那么做为sparkstreaming 我们至少要开启
      * 两个线程，因为其中一个线程用来接收数据，这样另外一个线程用来处理数据。
      */
    val conf=new SparkConf().setMaster("local[2]").setAppName("NetWordCount")
    /**
      * Seconds  指的是每次数据数据的时间范围 （bacth interval）
      */
    val  ssc=new StreamingContext(conf,Seconds(2));

/**
  * 根据key更新状态，需要设置 checkpoint来保存状态
  * 默认key的状态在内存中 有一份，在checkpoint目录中有一份。
  *
  *    多久会将内存中的数据（每一个key所对应的状态）写入到磁盘上一份呢？
  * 	      如果你的batchInterval小于10s  那么10s会将内存中的数据写入到磁盘一份
  * 	      如果bacthInterval 大于10s，那么就以bacthInterval为准
  *
  *    这样做是为了防止频繁的写HDFS
  */

    ssc.checkpoint(".")
    val fileDS=ssc.socketTextStream("hadoop1", 9999)
    val wordcount=fileDS.flatMap { line => line.split("\t") }
      .map { word => (word,1) }
    /**
      * updateFunc: (Seq[Int], Option[S]) => Option[S]
      * updateFunc 这是一个匿名函数
      *  (Seq[Int], Option[S]) 两个参数
      *  Option[S] 返回值
      *  首先我们考虑一个问题
      *  wordDS  做的bykey的计算，说明里面的内容是tuple类型，是键值对的形式，说白了是不是
      *  就是【K V】
      *  wordDS[K,V]
      *  (Seq[Int], Option[S])
      *  参数一：Seq[Int] Seq代表的是一个集合，int代表的是V的数据类型
      *  			---分组的操作，key相同的为一组 (hadoop,{1,1,1,1})
      *  参数二：Option[S] S代表的是中间状态State的数据类型，S对于我们的这个wordcount例子来讲，应该是
      *  int类型。中间状态存储的是单词出现的次数。 hadoop -> 4
      *
      *  Option[S] 返回值  应该跟中间状态一样吧。
      *  Option Some/None
      *
      */

    wordcount.updateStateByKey((values:Seq[Int],state:Option[Int]) =>{
      val currentCount= values.sum;  //获取此次本单词出现的次数
      val count=state.getOrElse(0);//获取上一次的结果 也就是中间状态

      //updateStatebykey要求返回一个option some是option的子类。 这里保存到checkpoint里面
      Some(currentCount+count);

   })
//    wordcount.mapWithState()
  }
}
