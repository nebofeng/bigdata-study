package pers.nebo.sparksql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.Queue
/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/8/28
  * @ des :
  */
object PageTop3Daily {

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("PageTop3Daily")
    val sc= new SparkContext(conf)
    val filepath=this.getClass.getResource("/sparksql/log.txt").getPath
    val spark=SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._
    val log=sc.textFile(filepath)


    import scala.collection.mutable.Map
    val queryParamMap:Map[String,List[String]]=Map();
    val list=List("beijing");
    queryParamMap+=("city" -> list)
    /**
      * 将查询条件给广播出去
      */
    val queryParamMapbroadcast= sc.broadcast(queryParamMap)

    /**
      * 需求一 过滤出符合条件的数据
      */
    val filterRDD=log.filter {
        log => {
        val logArray= log.split("\t")
        val city=logArray(3)
        val queryParam= queryParamMapbroadcast.value
        val citys=queryParam.get("city").get
        if(citys.size > 0 && citys.contains(city)){
          true
        }else{
          false
        }
      }
    }


    /**
      * 需求二2、统计出每天搜索uv排名前3的搜索页面
      * 1）（date_page,ip）  groupby
      *    (date_page,ips)  ->  (date_page,uv)
      * 2)(date_page,uv) -> (date,page,uv) ->dataframe
      * rownuber
      */
    val date_page_ips=filterRDD.map {
        log => {
        val logArray= log.split("\t")
        val ip=logArray(0);
        val date=logArray(1);
        val page=logArray(2);
        (date+"_"+page,ip)
      }
    }.groupByKey()

    /**
      * 对ip进行去重 =》uv
      */

    val date_page_uv=date_page_ips.map(tuple =>{
      val date_page=tuple._1
      val ips=tuple._2.iterator;
      import scala.collection.mutable.Set
      val set:Set[String]=Set()
      while(ips.hasNext){
        val ip=ips.next();
        set.add(ip)
      }
      (date_page,set.size)
    })
    /**
      * rdd -> dataframe
      */
    val rowRDD= date_page_uv.map(result =>{
      val date_page=  result._1;
      val date=date_page.split("_")(0)
      val page=date_page.split("_")(1)
      val uv=result._2
      Row(date,page,uv)
    })

    val schema=StructType(
      Array(
        StructField("date",StringType,true),
        StructField("page",StringType,true),
        StructField("uv",IntegerType,true)
      )
    )

    val date_page_uv_df=  spark.createDataFrame(rowRDD, schema)
    date_page_uv_df.createOrReplaceTempView("date_ip_uv")
    val sql="""
      select date,page,uv from
      (select date,page,uv,row_number() over (partition by date order by uv desc) rank from  date_ip_uv) tmp
      where tmp.rank <=3
      """
    val top3Daily = spark.sql(sql)
    println("=============================top 3 daily=========================")
    top3Daily.show()

    /**统计出每天搜索uv排名前3的搜索页面
      * 需求三 按照每天的top3页面的uv搜索总次数，倒序排序
      */

    val totalUVRDD=top3Daily.rdd.map(
      row=>{
        val date=row(0)
        val page=row(1)
        val uv=row(2)
        (date,page+"_"+uv)
      }
    ).groupByKey()
       .map(tuple =>{
        val date=tuple._1
        val page_uvs=tuple._2.iterator
        var totalUV=0;
        var str=date;
        while(page_uvs.hasNext){
          val page_uv=page_uvs.next();
          val uv=page_uv.split("_")(1).toInt
          totalUV+=uv
          str+=","+page_uv
        }

        (totalUV,str)
      }).sortByKey(false)

    /**
      * totalUVRDD
      * k    v
      * k:totaluv  30
      * v: 201610,page1_12,page2_10,page3_8
      *
      */
    val rowresultRDD=totalUVRDD.flatMap(tuple =>{
      val date_page_uv= tuple._2.toString().split(",")
      val date=date_page_uv(0);
      val ip_uv1=date_page_uv(1)
      val ip_uv2=date_page_uv(2)
      val ip_uv3=date_page_uv(3)

      val queue:Queue[Row]=Queue();
      val row1=Row(date,ip_uv1.split("_")(0),ip_uv1.split("_")(1).toInt)
      val row2=Row(date,ip_uv2.split("_")(0),ip_uv2.split("_")(1).toInt)
      val row3=Row(date,ip_uv3.split("_")(0),ip_uv3.split("_")(1).toInt)
      queue+=row1;
      queue+=row2;
      queue+=row3;
      queue.iterator
    })

    println("========================================")
    val top3dailyDF= spark.createDataFrame(rowresultRDD, schema)
    top3dailyDF.show();
    //top3dailyDF.("daily_top3")
  }






}
