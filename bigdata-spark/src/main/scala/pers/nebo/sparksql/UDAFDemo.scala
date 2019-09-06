package pers.nebo.sparksql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, StructField, StructType}

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/8/23
  * @ des :
  */
/**
  * 思考：
  * 想计算工资的平均值。
  * 首先计算出所有人的工资的和
  * 然后再计算出有多少人
  * 工资的和/人数=平均工资
  */
object UDAFDemo  extends UserDefinedAggregateFunction{

  /**
    * 定义缓存字段的名称和数据类型
    */
  def bufferSchema:StructType = StructType(
    StructField("total",DoubleType,true)::StructField("count",IntegerType,true)::Nil
  )
  /**
    * 定义输出的数据类型
    */
  def dataType: DataType = DoubleType
  /**
    * 是否指定唯一
    */
  def deterministic: Boolean = true
  /**
    * 最后的目标就是做如下的计算
    */
  def evaluate(buffer: Row): Any = {
    val  total= buffer.getDouble(0)
    val count=buffer.getInt(1);
    total/count
  }
  /**
    * 对于参数计算的值进行初始化
    */
  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, 0.0)
    buffer.update(1, 0)
  }
  /**
    * 定义输入的数据类型
    */
  def inputSchema: StructType = StructType(
    StructField("salary",DoubleType,true)::Nil
  )
  /**
    * 进行全局的统计
    */
  def merge(buffer1: MutableAggregationBuffer,buffer2:Row): Unit = {
    val total1= buffer1.getDouble(0);
    val count1=buffer1.getInt(1);
    val total2= buffer2.getDouble(0);
    val count2=buffer2.getInt(1);
    buffer1.update(0, total1+total2)
    buffer1.update(1, count1+count2)
  }
  /**
    * 修改中间状态的值
    */
  def update(buffer: MutableAggregationBuffer,input:Row): Unit ={
    val total= buffer.getDouble(0);
    val count=buffer.getInt(1);
    val currentsalary=input.getDouble(0) //salary
    buffer.update(0, total+currentsalary)
    buffer.update(1, count+1)
  }

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("UDAFDemo")
    val sc=new SparkContext(conf);


    val hiveContext=new HiveContext(sc);
    hiveContext.udf.register("salary_avg",UDAFDemo)
    hiveContext.sql("select salary_avg(salary) from worker").show()

    /**
      * 2.2.x
      */
    val spark=SparkSession.builder().config(conf).getOrCreate()
    spark.udf.register("salary_avg",UDAFDemo)
  }
}