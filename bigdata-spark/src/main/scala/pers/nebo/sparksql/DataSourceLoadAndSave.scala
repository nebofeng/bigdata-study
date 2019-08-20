package pers.nebo.sparksql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/8/20
  * @ des : DataSourceLoadAndSave
  */
object DataSourceLoadAndSave {
  def main(args: Array[String]): Unit = {




  }

  def loadAndSave(){
    val conf=new SparkConf().setAppName("RDD2DataFrameReflection")
    val sc=new SparkContext(conf);
    val sqlContext=new SQLContext(sc);
    //  sqlContext.read.json("person.json");
    //sparksql默认支持的是parquet文件格式
    val df=  sqlContext.read.load("examples/src/main/resources/users.parquet");
    //  sqlContext.read.format("json").load("person.json")
    //  sqlContext.read.format("parquet").load("users.parquet")
    //df保存的时候，如果不指定保存的文件格式，默认就是parquet
    df.select("name", "age").write.save("namesAndAges.parquet");
    //  df.select("name", "age").write.json("user.json")
    //   df.select("name", "age").write.format("json").save("user.json")
    df.select("name", "age").write.format("parquet").save("user.parquet")

    df.select("name", "age").write.mode(SaveMode.ErrorIfExists).format("json").save("hdfs://hadoop1:9000/user.json");

    /**
      * 保存结果文件的时候的策略
      *
      */
  }

  def loadAndSave_new (){
    val conf=new SparkConf().setAppName("RDD2DataFrameReflection")
    val sc=new SparkContext(conf);
    val spark=SparkSession.builder().config(conf).getOrCreate();

    //  sqlContext.read.json("person.json");
    //sparksql默认支持的是parquet文件格式
    val df=  spark.read.load("examples/src/main/resources/users.parquet");
    //  sqlContext.read.format("json").load("person.json")
    //  sqlContext.read.format("parquet").load("users.parquet")
    //df保存的时候，如果不指定保存的文件格式，默认就是parquet
    df.select("name", "age").write.save("namesAndAges.parquet");
    //  df.select("name", "age").write.json("user.json")
    //   df.select("name", "age").write.format("json").save("user.json")
    df.select("name", "age").write.format("parquet").save("user.parquet")

    df.select("name", "age").write.mode(SaveMode.ErrorIfExists).format("json").save("hdfs://hadoop1:9000/user.json");

    /**
      * 保存结果文件的时候的策略
      *
      */
  }
}
