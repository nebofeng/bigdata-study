package pers.nebo.sparkstreaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/8/31
  * @ des : DriverHA
  */
object DriverHATest {
  def main(args: Array[String]): Unit = {
    val checkpointDirectory="kafka/";

    def functionToCreateContext(): StreamingContext = {
      val conf=new SparkConf().setMaster("local[2]").setAppName("KafkaOperation")
      val sc=new SparkContext(conf);
      val  ssc=new StreamingContext(sc,Seconds(2));
      ssc.checkpoint(checkpointDirectory)   // set checkpoint directory
      ssc
    }

    val conf=new SparkConf()
    val ssc = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext _)

    ssc.checkpoint(checkpointDirectory)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("topicA", "topicB")
    val kafkaDS = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    ).map(record => (record.value))
//    val kafkaDS= KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)
//      .map(_._2)

    val wordcountDS= kafkaDS.flatMap { line => line.split("\t") }
      .map { word => (word,1) }
      .reduceByKey(_+_)//window  mapwithstate updatewithstateByKey topK
    wordcountDS.print();
    ssc.start();
    ssc.awaitTermination();
  }

}
