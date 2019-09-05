package pers.nebo.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
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

    val pairsDS= fileDS.map { log => (log.split(",")(1)+"_"+log.split(",")(2) ,1)}

    val pairsCountsDS = pairsDS.reduceByKeyAndWindow((v1:Int,v2:Int)=>(v1+v2),Seconds(60),Seconds(10))


    val structType=StructType(
      Array(
        StructField("category",StringType,true),
        StructField("product",StringType,true),
        StructField("count",IntegerType,true)  ))




    val spark=SparkSession.builder().getOrCreate()

    pairsCountsDS.foreachRDD{
          //excute in driver

       rdd=>{
         val rowRDD=rdd.map{
           tuple=>{
             val category=   tuple._1.split("_")(0);
             val product=   tuple._1.split("_")(1);
             val count=tuple._2;
             Row(category,product,count)

           }
         }


        val rddDF=spark.createDataFrame(rowRDD,structType)
        rddDF.createOrReplaceTempView("demo_table")

         val sql="""
        select category,product,count from
        (select category,product,count,row_number() over(partition by category order by count desc) rank
        from product_count) tmp
        where tmp.rank <= 3

        """
         val topN= spark.sql(sql)

         topN.show()


       }

    }


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
