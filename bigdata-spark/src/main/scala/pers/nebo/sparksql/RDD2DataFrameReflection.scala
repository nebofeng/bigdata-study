package pers.nebo.sparksql
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/8/19
  * @ des : RDD2DataFrameReflection
  *
  * 通过反射的方式： 1. 首先将rdd 转为自定义类型RDD
  *               2. 再使用rdd.toDF()
  */
case class PersonScala(age:Int,name:String)

object RDD2DataFrameReflection {
  def main(args: Array[String]): Unit = {
     val sc = new SparkConf().setMaster("local")
       .setAppName("");
     val scontext=new SparkContext(sc);

    val sparkSession=SparkSession.builder().config(sc)
                    .getOrCreate();

    import sparkSession.implicits._
    val personRDD= scontext.textFile("hdfs://hadoop1:9000/examples/src/main/resources/people.txt", 1)
      .map { line => line.split(",") }.map { p => PersonScala(p(1).trim().toInt,p(0)) }
    val personDF=personRDD.toDF()
    personDF.registerTempTable("person")
    val personDataframe=sparkSession.sql("select name,age from person where age > 13 and age <= 19")
    personDataframe.rdd.foreach { row => println(row.getString(0)+"  "+row.getString(1)) }

    personDataframe.rdd.saveAsTextFile("hdfs://hadoop1:9000/reflectionresult/")



  }

}
