package pers.nebo.sparksql

import org.apache.spark.sql.SparkSession

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/9/1
  * @ des :
  */
object DataSetTypedTransformationOperation {

  case class Student(name:String,age:Long,classID:Long,gender:String,isnew:String)
  case class Classes(id:Long,classname:String)
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("DataSetTypedTransformationOperation")
      .master("local")
      .getOrCreate();
    //导入隐士转换
    import spark.implicits._
    import org.apache.spark.sql.functions._
    //DataFrame=DataSet[row]
    val studentDF=spark.read.json("C:\\Users\\Administrator.USER-20160518JB\\Desktop\\student.json")
    val studentDF2=spark.read.json("C:\\Users\\Administrator.USER-20160518JB\\Desktop\\student2.json")
    val classDF=spark.read.json("C:\\Users\\Administrator.USER-20160518JB\\Desktop\\class.json")
    val studentDS=studentDF.as[Student];
    val studentDS2=studentDF2.as[Student];
    val classDS=classDF.as[Classes];

    //coalesce和repartition
    //这个两个操作都是用来重新定义分区的
    //coalesce: 只能用于减少分区的数量，而且可以选择不发生shuffle 其实说白了他就是合并分区
    //repartition:可以增加分区，也可以减少分区，必须会发生shuffle，相当于就是进行重新分区

    println(studentDS.rdd.partitions.size);

    val studentDSrepartition= studentDS.repartition(7);
    println(studentDSrepartition.rdd.partitions.size);

    val studentDScoalesece=studentDS.coalesce(3);
    println(studentDScoalesece.rdd.partitions.size)

    /**
      * distinc 和 dropDuplelicates
      * 这两个算子操作都是用来去重的
      * distinc: 是根据每一条数据，进行完整内容的比对，然后进行去重
      * dropDuplelicates ： 可以根据指定的字段进行去重
      */
    studentDS.distinct().show();

    studentDS.dropDuplicates(Seq("name")).show();

    /**
      * except : 获取当前Dataset中有，但是另外一个Dataset中没有的元素
      * filter ：根据逻辑，如果返回true，那么就保留该元素，否则就过滤该元素
      * intersect ： 获取两个Dataset之间的交集
      *
      */
    studentDS.except(studentDS2).show();
    studentDS.filter( student => student.age > 40).show()
    studentDS.intersect(studentDS2).show();

    /**
      * map： 将数据集中的每一条数据都做一个映射，返回一条新的数据
      * flatMap ： 数据集中的每一条数据都可以返回多条数据
      * mapPartitions ：一次性对一个partition的数据进行操作
      *
      */

    studentDS.map( student =>(student.name,student.age + 1)).show()
    classDS.flatMap(classes => Seq(Classes(classes.id + 1,classes.classname+"+a"),Classes(classes.id + 2,classes.classname+"+b")))
    studentDS.mapPartitions( student =>{
      val result= scala.collection.mutable.ArrayBuffer[(String,Long)]();
      while(student.hasNext){
        val s=  student.next();
        result +=((s.name,s.age+1))
      }
      result.iterator

    })

    studentDS.joinWith(classDS,$"classID" === $"id").show();


    studentDS.sort($"age".desc).show();

  }

}
