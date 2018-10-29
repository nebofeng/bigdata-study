package pers.nebo.spark

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe



object WordCount {
  def main(args: Array[String]): Unit = {
    //初始化，创建SparkSession
    val spark = SparkSession.builder().appName("sskt").master("local[2]").getOrCreate()
    //初始化，创建sparkContext
    val sc = spark.sparkContext
    //初始化，创建StreamingContext，batchDuration为1秒
    val ssc = new StreamingContext(sc, Seconds(5))

    //开启checkpoint机制
    ssc.checkpoint("E:/TEMP")

    //kafka集群地址
    val server = "139.199.172.112:9092,123.207.241.42:9092,118.89.61.19:9092"

    //配置消费者
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> server, //kafka集群地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "UpdateStateBykeyWordCount", //消费者组名
      "auto.offset.reset" -> "latest", //latest自动重置偏移量为最新的偏移量   earliest 、none
      "enable.auto.commit" -> (false: java.lang.Boolean)) //如果是true，则这个消费者的偏移量会在后台自动提交
    val topics = Array("kafkawordcount") //消费主题

    //基于Direct方式创建DStream
    val stream = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    //开始执行WordCount程序

    //以空格为切分符切分单词，并转化为 (word,1)形式
    val words = stream.flatMap(_.value().split(" ")).map((_, 1))
    val wordCounts = words.updateStateByKey(
      //每个单词每次batch计算的时候都会调用这个函数
      //第一个参数为每个key对应的新的值，可能有多个，比如(hello,1)(hello,1),那么values为(1,1)
      //第二个参数为这个key对应的之前的状态
      (values: Seq[Int], state: Option[Int]) => {

        var newValue = state.getOrElse(0)
        values.foreach(newValue += _)
        Option(newValue)

      })
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()

  }


}
