package pers.nebo.structuredstreaming

import org.apache.spark.sql.SparkSession

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/9/1
  * @ des :
  */
object StructruedStreamingDemo {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("WordCount")
      .getOrCreate()
    import spark.implicits._

    val lines=spark.readStream
      .format("socket")
      .option("host", "hadoop1")
      .option("port", 9999)
      .load()

    val words= lines.as[String].flatMap(_.split("\t"))
    val wordCounts=words.groupBy("value").count();
    val query= wordCounts.writeStream
      .outputMode("complete") //
      .format("console")
      .start()

    query.awaitTermination();

  }
}
