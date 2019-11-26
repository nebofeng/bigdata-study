package pers.nebo.sparkcore.pvuvdemo

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/11/25
  * @ des :
  */
object SparkPVUV {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("test")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("/DATA/pvuvdata/")


    //每个网址，地区访问量的top3 及访问量
    val site_local_a=lines.map(line=>{
        val item=line.split("\t")
              (item(5)+"_"+item(1),1)
        }
       )
    //返回 网址 地区 的tuple

//    val site_local_result = site_local_a.reduceByKey((a:Int,b:Int)=>{a+b})
    val site_local_result = site_local_a.reduceByKey(_+_)

    val local_result=site_local_result.map(line=>{
             val site_local=line._1.split("_")
            (site_local(0),(site_local(1),line._2))

    }).groupByKey().map(line=>{

        val site=line._1
        val tuples=line._2.iterator.toList.sortBy(elem=> -elem._2).take(3)
      (site,tuples)
    }).foreach(println)







    //每个网址的每个地区访问量 ，由大到小排序
    val site_local = lines.map(line=>{(line.split("\t")(5),line.split("\t")(1))})
    // 网址为key  将地区作为 iterable ,  然后遍历 iterable 。统计 iterable 中地区的数目 。
    val site_localIterable = site_local.groupByKey()
    val result = site_localIterable.map(one => {

      val localMap = mutable.Map[String, Int]()
      val site = one._1
      val localIter = one._2.iterator

     // 统计 iterable 中地区的数目 。 放置到local map 中  地区  数量
      while (localIter.hasNext) {
        val local = localIter.next()
        if (localMap.contains(local)) {
          val value = localMap.get(local).get
          localMap.put(local, value + 1)
        } else {
          localMap.put(local, 1)
        }
      }
      //取出前三个  地区及对应的 数量
      val tuples: List[(String, Int)] = localMap.toList.sortBy(one => {
        -one._2
      })

      //封装到 对应的 site 返回结果中
      if(tuples.size>3){
        val returnList = new ListBuffer[(String, Int)]()
        for(i <- 0 to 2){
          returnList.append(tuples(i))
        }
        (site, returnList)
      }else{
        (site, tuples)
      }
    })
    result.foreach(println)

    //pv
    //    lines.map(line=>{(line.split("\t")(5),1)}).reduceByKey((v1:Int,v2:Int)=>{
    //      v1+v2
    //    }).sortBy(tp=>{tp._2},false).foreach(println)

    //uv
    //    lines.map(line=>{line.split("\t")(0)+"_"+line.split("\t")(5)})
    //      .distinct()
    //      .map(one=>{(one.split("_")(1),1)})
    //      .reduceByKey(_+_)
    //      .sortBy(_._2,false)
    //      .foreach(println)


  }
}
