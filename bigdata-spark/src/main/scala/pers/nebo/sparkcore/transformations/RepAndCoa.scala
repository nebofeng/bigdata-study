package pers.nebo.sparkcore.transformations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/11/24
  * @ des :
  */
object  RepAndCoa {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("test")
    val sc = new SparkContext(conf)

    /**
      * countByKey
      * countByValue
      * reduce
      *
      */
    //    val rdd1 = sc.parallelize(List[(String,Int)](
    //              ("a",100),
    //              ("a",200),
    //              ("b",2),
    //              ("c",300),
    //              ("d",100),
    //              ("a",100)
    //    ))
    //    val rdd1 = sc.parallelize(List[Int](1,2,3,4,5,5))
    //    val result: collection.Map[Int, Long] = rdd1.countByValue()
    //    result.foreach(println)

    //    val result: collection.Map[String, Long] = rdd1.countByKey()
    //    result.foreach(println)


    //    val rdd = sc.parallelize(List[Int](1,2,3,4,5))
    //    val result: Int = rdd.reduce((v1, v2)=>{v1+v2})
    //    println(result)
    /**
      * groupByKey
      */
    //    val rdd1 = sc.parallelize(List[(String,Int)](("a",100),("a",200),("b",2),("b",200)))
    //    rdd1.groupByKey().foreach(println)
    /**
      * zip ,zipWithIndex
      */
    //    val rdd1 = sc.parallelize(List[String]("zhangsan","lisi","wangwu","maliu"))
    //    val rdd2 = sc.parallelize(List[String]("100","200","300","400"))

    // zipWithIndex 与当前元素的下标（value）压缩在一起
    //    val result: RDD[(String, Long)] = rdd1.zipWithIndex()
    //    result.foreach(println)

    //    val result: RDD[(String, String)] = rdd1.zip(rdd2)
    //    result.foreach(println)

//    /**
//      * repartition + coalesce
//      */
//        val rdd1 = sc.parallelize(List[String](
//          "love1","love2","love3","love4",
//          "love5","love6","love7","love8",
//          "love9","love10","love11","love12"
//        ),3)
//        val rdd2 = rdd1.mapPartitionsWithIndex((index,iter)=>{
//          val list = new ListBuffer[String]()
//          while(iter.hasNext){
//            val one = iter.next()
//            list.+=(s"rdd1 partition = 【$index】,value = 【$one】")
//          }
//          list.iterator
//        })

    //    /**
    //      * repartition
    //      * 可以增多分区，也可以减少分区,会产生shuffle
    //      * 使用repartition的时候原来的分组数据去往了其他分区数据 ，产生了shuffle 宽依赖算子


    //      * coalesce
    //      * 可以增多分区，也可以减少分区
    //      *
    //      * 当coalesce 由少的分区分到多的分区时，不让产生shuffle ,不起作用. 参数为true的时候与repartition一样，
    //      * 当它为false 的时候无法产生shuffle  （存在空分区的情况，这里是直接没有新增分区）
    //      * repartition 常用于增多分区，coalesce 常用于减少分区
    //      */
    ////    val rdd3 = rdd2.repartition(2)
    //    val rdd3 = rdd2.coalesce(4,true)
    //    println(s"rdd3 partition length = ${rdd3.getNumPartitions}")
    //    val rdd4 = rdd3.mapPartitionsWithIndex((index,iter)=>{
    //      val list = new ListBuffer[String]()
    //      while(iter.hasNext){
    //        val one = iter.next()
    //        list.+=(s"rdd3 partition = 【$index】,value = 【$one】")
    //      }
    //      list.iterator
    //    })
    //    val result = rdd4.collect()
    //    result.foreach(println)
    //
    ////        rdd2.foreach(println)
    //

  }


}
