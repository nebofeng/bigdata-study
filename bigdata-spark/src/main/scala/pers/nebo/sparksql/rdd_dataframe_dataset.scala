package pers.nebo.sparksql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/9/1
  * @ des :
  */
class rdd_dataframe_dataset {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("rdd_dataframe_dataset")
      .master("local")
      .getOrCreate();
    /**
    ------ 优点:

编译时类型安全
编译时就能检查出类型错误
面向对象的编程风格
直接通过类名点的方式来操作数据
-------缺点:

序列化和反序列化的性能开销
无论是集群间的通信, 还是IO操作都需要对对象的结构和数据进行序列化和反序列化.
GC的性能开销
频繁的创建和销毁对象, 势必会增加GC
      *
      *
      */
    //=====================================RDD============================================
    case class Person(id: Int, age: Int)
    val idAgeRDDPerson = spark.sparkContext.parallelize(Array(Person(1, 30), Person(2, 29), Person(3, 21)))

    // 优点1
    //  idAgeRDDPerson.filter(_.age > "") // 编译时报错, int不能跟String比

    // 优点2
    idAgeRDDPerson.filter(_.age > 25) // 直接操作一个个的person对象

    /**
      *
      * DataFrame引入了schema和off-heap
schema : RDD每一行的数据, 结构都是一样的. 这个结构就存储在schema中. Spark通过schame就能够读懂数据,
 因此在通信和IO时就只需要序列化和反序列化数据, 而结构的部分就可以省略了.
off-heap : 意味着JVM堆以外的内存, 这些内存直接受操作系统管理（而不是JVM）。
Spark能够以二进制的形式序列化数据(不包括结构)到off-heap中, 当要操作数据时, 就直接操作off-heap内存.
由于Spark理解schema, 所以知道该如何操作.
off-heap就像地盘, schema就像地图, Spark有地图又有自己地盘了, 就可以自己说了算了, 不再受JVM的限制,
 也就不再收GC的困扰了.
通过schema和off-heap, DataFrame解决了RDD的缺点, 但是却丢了RDD的优点. DataFrame不是类型安全的,
API也不是面向对象风格的.
      *
      *
      */
    //================================dataframe============================================
    val schema = StructType(Array(StructField("id", DataTypes.IntegerType), StructField("age", DataTypes.IntegerType)))
    val idAgeRDDRow = spark.sparkContext.parallelize(Array(Row(1, 30), Row(2, 29), Row(4, 21)))
    val idAgeDF =  spark.createDataFrame(idAgeRDDRow, schema)
    // API不是面向对象的
    idAgeDF.filter(idAgeDF.col("age") > 25)
    // 不会报错, DataFrame不是编译时类型安全的
    idAgeDF.filter(idAgeDF.col("age") > "")

    //================================DataSet==============================================
    /**
      * RDD API 存储的是原始的 JVM 对象，提供丰富的函数式运算符，使用起来很灵活，
      * 但是由于缺乏类型信息很难对它的执行过程优化。DataFrame API 存储的是 Row 对象，
      * 提供了基于表达式的运算以及逻辑查询计划，很方便做优化，并且执行起来速度很快，但是却不如 RDD API 灵活。
        DataSet API 则充分结合了二者的优势，既允许用户很方便的操作领域对象又拥有 SQL 执行引擎的高性能表现。
              本质上来说 DataSet API 相当于 RDD + Encoder， Encoder 可以将原始的 JVM对象高效的转化为二进制格式，
              使得可以后续对其进行更多的处理。
      */
  }
}
