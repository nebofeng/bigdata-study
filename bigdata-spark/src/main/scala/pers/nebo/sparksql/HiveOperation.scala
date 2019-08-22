package pers.nebo.sparksql

import java.io.File
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/8/22
  * @ des : HiveOperation
  */
object   HiveOperation {

  def main(args: Array[String]): Unit = {

  }

  def hiveOperation_old(){
    /**
      * 1.6.x
      */
    val conf=new SparkConf().setAppName("HiveOperation")
    val sc=new SparkContext(conf);
    //SQLContext
    val hiveContext=new HiveContext(sc);
    hiveContext.sql("use xtwy");
    hiveContext.sql("drop table if exists student");
    hiveContext.sql("drop table if exists teernages_students");
    hiveContext.sql("create table student(name string,age int) row format delimited fields terminated by ','")
    hiveContext.sql("load data local inpath '/usr/local/soft/spark/examples/src/main/resources/student.txt' into table student")

    import hiveContext.implicits._
    val teernagesdf= hiveContext.sql("select name,age from student where age >= 13 and age <= 19")
//    teernagesdf.saveAsTable("teernages_students");
    //新版本api没有saveAsTable方法




  }

  def hiveOperation_new(){

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    import spark.sql
    val sqlDF = sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")
    /*
    df.registerTempTable(tempName)
    hsc.sql(s"""
            CREATE TABLE $tableName (
            // field definitions   )
            STORED AS $format """)

    hsc.sql(s"INSERT INTO TABLE $tableName SELECT * FROM $tempName")


   */
     sqlDF.write.saveAsTable("db_name.table_name")
  }

}
