package pers.nebo.sparksql

import org.apache.spark.sql.SparkSession

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/9/1
  * @ des : DataSetActionOperation
  */
object DataSetActionOperation {


  val spark=SparkSession
    .builder()
    .appName("DataSetActionOperation")
    .master("local")
    .getOrCreate();
  //导入隐士转换
  import spark.implicits._
  import org.apache.spark.sql.functions._
  //DataFrame=DataSet[row]
  val studentDF=spark.read.json("C:\\Users\\Administrator.USER-20160518JB\\Desktop\\student.json")

  //collect 将分布式存在集群上数据集中的所有数据都获取到driver 端统一处理。
  studentDF.collect().foreach { println(_) }

  //count ,统计dataset的个数
  println(studentDF.count());

  //first/head 获取数据集中第一条数据
  println(studentDF.first());
  println(studentDF.head());

  //foreach 这个操作是遍历集合中每一条数据，但是这个跟Collect不同，因为Collect是讲数据拉取到driver进行操作
  //而foreach是将计算操作推到集群上去分布式的执行
  //foreach(println(_)) 这种方法，真正执行的时候，我们是观察不到结果的，因为输出的结果到了
  //分布式的集群中。

  studentDF.foreach { println(_) }
  //show 默认将dataset集合中前二十条数据打印出来
  studentDF.show()

  //take. 从dataset数据集中获取指定条数
  studentDF.take(3).foreach { println(_) }


}
