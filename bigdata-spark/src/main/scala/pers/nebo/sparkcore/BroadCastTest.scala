package pers.nebo.sparkcore

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/12/1
  * @ des :
  */
object BroadCastTest {


  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
    conf.setMaster("local")
    conf.setAppName("TEST")

    val sc = new SparkContext(conf)

    val list=List[String]("demo2","demo1")
    val bcList:Broadcast[List[String]]=sc.broadcast(list)
    val nameRDD= sc.parallelize(List[String]("demo1","demo2","demo3"))

    val result=nameRDD.filter(name=>{
      val innerList:List[String]=bcList.value
      !innerList.contains(name)

    })

    result.foreach(println)


  }

}
