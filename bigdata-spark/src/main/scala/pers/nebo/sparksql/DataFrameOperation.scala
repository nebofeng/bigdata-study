package pers.nebo.sparksql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/8/19
  * @ des :Spark SQL DataFrame操作
  */
object DataFrameOperation {

  def main(args: Array[String]): Unit = {
    showtest()
  }


  def showtest() {

    val conf = new SparkConf().setAppName("DataFrameOperation").setMaster("local")


    //create sqlcontext
    /*
       val sc = new SparkContext(conf);
       val sqlContext=new SQLContext(sc);  1.6x 需要初始化spark context
       val df=sqlContext.read.json("hdfs://hadoop1:9000/examples/src/main/resources/people.json")

      */
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config(conf)
      // .config("spark.some.config.option", "some-value")
      .getOrCreate()
    //这里虽然是高可用集群，因为需要将配置文件加载才能识别，所以为了方便直接写当前活跃状态的nn地址
    val df = spark.read.json("hdfs://node1:9000/tmp/people.json")

    df.show()
    df.printSchema()
  }

}
