package pers.nebo.sparksql

import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable.ListBuffer
import scala.runtime.Nothing$

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/8/20
  * @ des : 使用编程的方式将RDD 转为DataFrame Scala版
  *  1. 创建row 类型 rdd
  *  2. 创建 schema
  *  需要注意的是动态创建row的顺序，需要与schema 一致。
  */
object RDD2DataFrameProgrammactically {
  def main(args: Array[String]): Unit = {

    //创建spark conf
    val conf= new SparkConf().setAppName("RDD2DataFrameProgrammactically")
      .setMaster("local");
    val  sc =new SparkContext(conf)
    //创建sparksession
    val spark=new sql.SparkSession.Builder().config(conf)
      .getOrCreate();

    val personRDD = sc.textFile("");


    val schemaString="name	age";
    val schema=StructType(
      schemaString.split("\t").map { fieldsName => StructField(fieldsName,StringType,true) }
    )


    //create rowrdd
    val rowRDD=personRDD.map { line => line.split(",") }.map { p => Row(p(0),p(1)) }
    val personDF=spark.createDataFrame(rowRDD, schema)
    personDF.registerTempTable("person");
    val personDataframe=spark.sql("select name,age from person where age > 13 and age <= 19")

    personDataframe.rdd.foreach { row => println(row.getString(0)+"=>  "+row.getString(1)) }
//    personDataframe.rdd.saveAsTextFile("hdfs://hadoop1:9000/RDD2DataFrameProgrammatically/")




  }

}
