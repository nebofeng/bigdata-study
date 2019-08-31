package pers.nebo.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/8/30
  * @ des :
  */
object reduceByKeyAndWindowDemo {
  def main(args: Array[String]): Unit = {

    val conf=new SparkConf().setMaster("local[2]").setAppName("TransformaDemo")
    val  ssc=new StreamingContext(conf,Seconds(2));
    val fileDS=ssc.socketTextStream("192.168.32.110", 9999);
    val wordcountDS=fileDS.flatMap { line => line.split("\t") }
      .map { word => (word,1) }  //(_+_)
      .reduceByKeyAndWindow((x:Int,y:Int) =>{x+ y},Seconds(6),Seconds(4))

    wordcountDS.print();
    ssc.start();
    ssc.awaitTermination();
    /*
     * hadoop	hadoop 不在统计范围之内
     * hadoop	hadoop
     * hadoop	hadoop  hadoop,6
     * hadoop	hadoop  hadoop,6
     * hadoop	hadoop
     * hadoop	hadoop
     *
     * */

  }

}
