package pers.nebo.sparkcore.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/9/23
  * @ des :
  */
object aggregateDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkRDDTest2").setMaster("local")
    val sc = new SparkContext(conf)

    //指定为2个分区
    val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7), 2)
    //设定一个函数，设定分区的ID索引，数值
    val func1 = (index: Int, iter: Iterator[(Int)]) => {
      iter.toList.map(x => "[partID:" + index + ", val: " + x + "]").iterator
    }

    //查看每个分区的信息
    val res1 = rdd1.mapPartitionsWithIndex(func1)
    res1.foreach(line=>{printf(line)})



    //用aggregate，指定初始值，对rdd1进行聚合操作,先进行局部求和，再进行全局求和
    val res2 = rdd1.aggregate(0)(_ + _, _ + _)
    printf("全部求和: " +res2)
    //将局部分区中最大的数找出来再进行求和
    val res3 = rdd1.aggregate(0)(math.max(_, _), _ + _)
    printf("两个分区的最大值求和： "+res3)



    val rdd =sc.parallelize(List(1,2,3,4,5),2)

    val num=rdd.aggregate(0)((x,y)=>math.max(x,y),(x,y)=>(x+y))
    val num1=rdd.aggregate(0)(math.max(_,_),(_+_))
    printf("分区最大值相加"+num)


    val rddLength =sc.parallelize(List("12","23","345","456","4567"),2)

    val lengthNum=rddLength.aggregate("")((x,y)=>math.max(x.length,y.length).toString,(x,y)=>(x+y))

    printf("分区字符长度最大值拼接"+lengthNum)



  }




}