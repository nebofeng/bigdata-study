package pers.nebo.sparkstreaming
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.flume.FlumeUtils
/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/8/31
  * @ des :FlumeWordcount
  */
object FlumeWordcount {
  def main(args: Array[String]): Unit = {

    val conf=new SparkConf().setMaster("local[2]").setAppName("FlumeWordcount")
    val sc=new SparkContext(conf);
    val  ssc=new StreamingContext(sc,Seconds(5));
    ssc.checkpoint(".")
    //  ReceiverInputDStream[SparkFlumeEvent]
    //flume event header,body
    //kafka message
    val flumeDS=  FlumeUtils.createPollingStream(ssc, "hadoop1", 9999)
      .map { event => new String(event.event.getBody.array()) }
    val wcDS= flumeDS.flatMap { line => line.split("\t") }
      .map { word => (word,1) }
      .reduceByKey(_+_);

    wcDS.print();
    ssc.start();
    ssc.awaitTermination();
  }

}
