package pers.nebo.sparkcore

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/12/2
  * @ des : spark core pipeline 验证 一条流
  */
object PipelineTest {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("test")
    val sc = new SparkContext(conf)

    val rdd1 =sc.parallelize(List[String]("demo1","demo2","demo3"))

    val rdd2=rdd1.map(line=>{
      println("****map ****"+line)
      line+"~"
    })

    val rdd3=rdd2.filter(line=>{
      println(s"===filter ===$line")
      true
    })

    rdd3.count()
  }

}
