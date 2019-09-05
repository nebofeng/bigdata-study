package pers.nebo.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/8/31
  * @ des : TopNOperation
  */
object TopNOperation {

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[2]").setAppName("TopNDemo")
    val ssc=new StreamingContext(conf,Seconds(6))

    val dataDS=ssc.socketTextStream("hadoop1", 9999)
    val winDow=dataDS.map(line=>(line,1)).reduceByKeyAndWindow(((a:Int,b:Int) => (a+b)),Seconds(60),Seconds(10))

    // reduceByKeyAndWindowDemo


  }

}
