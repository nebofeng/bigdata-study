package pers.nebo.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.CellUtil

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/8/22
  * @ des :
  */

case class Person(ID:String,age:String,name:String)
object HbaseOperation {
  def main(args: Array[String]): Unit = {

  }
  def hbaseOperation_old {

    val sparkconf=new SparkConf().setAppName("HbaseOperation")
    val sc=new SparkContext(sparkconf);
    val sqlContext=new SQLContext(sc);
    import sqlContext.implicits._
    /**
      * conf: Configuration,
      * fClass: Class[F], 表的格式
      * kClass: Class[K],
      * vClass: Class[V]):
      *
      * RDD[(K, V)]
      * 其实这个地方，如果有同学用mapreduce操作过hbase，那么这儿跟mapreduce操作hbase是一模一样的
      */
    val conf=HBaseConfiguration.create();
    conf.set("hbase.mapreduce.inputtable", "person")
    conf.set("hbase.zookeeper.qurom", "hadoop1:2181")
    conf.set("zookeeper.znode.parent", "/hbase")
    val PersonRDD= sc.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable] , classOf[Result])

    println("==========================count"+PersonRDD.count())
    /**
      * 把我们的数据封装成一个dataframe,然后注册成一张表，那么我们是不是可以用sql语句
      * 去操作hbase数据库！！！
      */
    val pRDD=PersonRDD.map(tuple =>{
      val rowkey= Bytes.toString( tuple._1.get)
      val result=tuple._2
      var rowStr=rowkey+","
      for(cell <- result.rawCells()){
        //  val f= Bytes.toString(CellUtil.cloneFamily(cell));
        val q= Bytes.toString(CellUtil.cloneQualifier(cell));
        val v= Bytes.toString(CellUtil.cloneValue(cell));
        rowStr+=v+","
      }
      //1,110,zhangxueyou  ,
      //2,120,liudehua
      rowStr.substring(0, rowStr.length()-1)
    })

    import sqlContext.implicits._
    val rowPersonRDD= pRDD.map { str => str.split(",") }
      .map { row => Person(row(0),row(1),row(2)) }

    val personDF= rowPersonRDD.toDF();
    personDF.registerTempTable("person")
    sqlContext.sql("select ID,name from person").show()
  }


  def hbaseOperation_new {

    val sparkconf=new SparkConf().setAppName("HbaseOperation")
    val sc=new SparkContext(sparkconf);
    val spark=SparkSession.builder().config(sparkconf).getOrCreate();

    /**
      * conf: Configuration,
      * fClass: Class[F], 表的格式
      * kClass: Class[K],
      * vClass: Class[V]):
      *
      * RDD[(K, V)]
      * 其实这个地方，如果有同学用mapreduce操作过hbase，那么这儿跟mapreduce操作hbase是一模一样的
      */
    val conf=HBaseConfiguration.create();
    conf.set("hbase.mapreduce.inputtable", "person")
    conf.set("hbase.zookeeper.qurom", "hadoop1:2181")
    conf.set("zookeeper.znode.parent", "/hbase")
    val PersonRDD= sc.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable] , classOf[Result])

    println("==========================count"+PersonRDD.count())
    /**
      * 把我们的数据封装成一个dataframe,然后注册成一张表，那么我们是不是可以用sql语句
      * 去操作hbase数据库！！！
      */
    val pRDD=PersonRDD.map(tuple =>{
      val rowkey= Bytes.toString( tuple._1.get)
      val result=tuple._2
      var rowStr=rowkey+","
      for(cell <- result.rawCells()){
        //  val f= Bytes.toString(CellUtil.cloneFamily(cell));
        val q= Bytes.toString(CellUtil.cloneQualifier(cell));
        val v= Bytes.toString(CellUtil.cloneValue(cell));
        rowStr+=v+","
      }
      //1,110,zhangxueyou  ,
      //2,120,liudehua
      rowStr.substring(0, rowStr.length()-1)
    })
    import spark.implicits._
    val rowPersonRDD= pRDD.map { str => str.split(",") }
      .map { row => Person(row(0),row(1),row(2)) }

    val personDF=rowPersonRDD.toDF()

    personDF.registerTempTable("person")
//    sqlContext.sql("select ID,name from person").show()
  }
}
