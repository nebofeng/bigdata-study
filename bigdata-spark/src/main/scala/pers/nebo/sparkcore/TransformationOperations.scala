package pers.nebo.sparkcore

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/8/2
  * @ des :
  */


class MyPartition(partitions:Int) extends Partitioner {
  def numPartitions: Int =partitions
  def getPartition(key: Any): Int=key.toString().hashCode() % 2
}

object TransformationOperations {

  def repartitionAndSortWithinPartitions(){
    val conf=new SparkConf().setMaster("local").setAppName("repartitionAndSortWithinPartitions")
    val sc=new SparkContext(conf);
    val number=Array(1,2,3,4,5,6,7,8);
    val numberRDD= sc.parallelize(number);
    numberRDD.map { x => (x,(new util.Random).nextInt(9)) }
      .repartitionAndSortWithinPartitions(new MyPartition(2))
      .foreach(result => println(result._1 + "  "+ result._2))
  }

  /**
    * 假设
    * p1:1234
    * p2:5678
    *
    * new map  长度可变的一个map集合
    *
    * <k,v>
    * k=p1+1
    *    *
    * 0 1
    * map(0_1,1)
    * map(0_2,2)
    * map(0_3,3)
    * map(0_4,4)
    * map(1_5,5)
    * map(1_6,6)
    * map(1_7,7)
    * map(1_8,8)
    *
    */
  def mapPartitionsWithIndex(){
    val conf=new SparkConf().setMaster("local").setAppName("mapPartitionsWithIndex")
    val sc=new SparkContext(conf);
    val number=Array(1,2,3,4,5,6,7,8);
    val numberRDD= sc.parallelize(number, 2);
    numberRDD.mapPartitionsWithIndex((x,iter) =>{
      import scala.collection.mutable.Map
      val map:Map[String,Int]= Map();
      while(iter.hasNext){
        val i=iter.next();
        map+=(x+"_"+i -> i);
      }
      map.iterator
    }, true)
      .foreach(result => println(result))
  }

  def aggrageteByKey(){
    val conf=new SparkConf().setMaster("local").setAppName("flatMap")
    val sc=new SparkContext(conf);
    val word=Array("you	jump","i	jump");
    val wordRDD=sc.parallelize(word);
    wordRDD.flatMap { x => x.split("\t") }
      .map { word => (word,1) }
      //第一个参数 初始值
      //第二个参数是一个函数，类似于Mapreduce里面 combine操作（在Map端做reduce的操作）局部汇总
      //第三个参数也是一个函数，全局汇总，就是一个reduce的操作
      .aggregateByKey(0)((a:Int,b:Int) => a+b, (a:Int,b:Int) => a+b)
      .foreach(result => println(result._1 + "  "+result._2 ))
  }


  def sample(){
    val conf=new SparkConf().setMaster("local").setAppName("mapPartitions")
    val sc=new SparkContext(conf);
    val pokers=Array("红桃A","红桃2","红桃3"
      ,"红桃4","红桃5","红桃6","红桃7","红桃8","红桃9","红桃10");
    val pokersRDD=sc.parallelize(pokers);
    pokersRDD.sample(true,0.1)
      .foreach { x => println(x) }
  }


  def mapPartitions(){
    val conf=new SparkConf().setMaster("local").setAppName("mapPartitions")
    val sc=new SparkContext(conf);
    val lista=Array(1,2,3);
    val listaRDD=sc.parallelize(lista);
    listaRDD.mapPartitions( number => number.map { x => "hello "+x }, true)
      .foreach { result => println(result) }
  }



  def cartesian(){
    val conf=new SparkConf().setMaster("local").setAppName("union")
    val sc=new SparkContext(conf);
    val lista=Array(1,2,3);
    val listb=Array("a","b","c");
    val listaRDD=sc.parallelize(lista);
    val listbRDD=sc.parallelize(listb);
    listaRDD.cartesian(listbRDD)
      .foreach(result => println(result))
  }

  def distinct(){
    val conf=new SparkConf().setMaster("local").setAppName("distinct")
    val sc=new SparkContext(conf);
    val lista=Array(1,2,3,4,4,5,6,6,7);
    val listaRDD=sc.parallelize(lista);
    listaRDD.distinct()
      .foreach { x => println(x) }
  }


  def union(){
    val conf=new SparkConf().setMaster("local").setAppName("union")
    val sc=new SparkContext(conf);
    val lista=Array(1,2,3,4);
    val listb=Array(4,5,6,7);
    val listaRDD=sc.parallelize(lista);
    val listbRDD=sc.parallelize(listb);
    listaRDD.union(listbRDD)
      .foreach { x => println(x) }
  }

  def intersection(){
    val conf=new SparkConf().setMaster("local").setAppName("intersection")
    val sc=new SparkContext(conf);
    val lista=Array(1,2,3,4);
    val listb=Array(4,5,6,7);
    val listaRDD=sc.parallelize(lista);
    val listbRDD=sc.parallelize(listb);
    listaRDD.intersection(listbRDD)
      .foreach { x => println(x) }
  }


  def cogroup(){
    val conf=new SparkConf().setMaster("local").setAppName("cogroup")
    val sc=new SparkContext(conf);
    val names=Array(
      Tuple2(1,"张无忌"),
      Tuple2(2,"周芷若"),
      Tuple2(3,"赵敏")
    );

    val scores=Array(
      Tuple2(1,56),
      Tuple2(2,77),
      Tuple2(3,89),
      Tuple2(1,57),
      Tuple2(2,78),
      Tuple2(3,90)
    );
    val namesRDD=sc.parallelize(names, 1)
    val scoresRDD=sc.parallelize(scores, 1)
    namesRDD.cogroup(scoresRDD)
      // ((Int, (Iterable[String], Iterable[Int])) parit  k  v(iter1 ,iter2)
      .foreach( result =>{
      println(result._1)
      result._2._1.foreach { x => println(x) }
      result._2._2.foreach { y => println(y) }
    })
  }


  def join(){
    val conf=new SparkConf().setMaster("local").setAppName("join")
    val sc=new SparkContext(conf);
    val names=Array(
      Tuple2(1,"张无忌"),
      Tuple2(2,"周芷若"),
      Tuple2(3,"赵敏")
    );
    val scores=Array(
      Tuple2(1,56),
      Tuple2(2,77),
      Tuple2(3,89)
    );
    val namesRDD=sc.parallelize(names, 1)
    val scoresRDD=sc.parallelize(scores, 1)
    namesRDD.join(scoresRDD)
      .foreach(result => println(
        "学号："+result._1 + "姓名"+result._2._1 + "分数："+result._2._2
      ))
    //pair  key  value zhangwuji 56


  }



  def sortByKey(){
    val conf=new SparkConf().setMaster("local").setAppName("reduceByKey")
    val sc=new SparkContext(conf);
    val names=Array(
      Tuple2(90,"张无忌"),
      Tuple2(92,"周芷若"),
      Tuple2(89,"赵敏"),
      Tuple2(93,"宋青书")
    );
    val namesRDD=sc.parallelize(names);
    namesRDD.sortByKey(false, 1)
      .foreach(result => println(result._2 + " : 分数： "+result._1))
  }

  def reduceByKey(){
    val conf=new SparkConf().setMaster("local").setAppName("reduceByKey")
    val sc=new SparkContext(conf);
    val names=Array(
      Tuple2("峨眉",30),
      Tuple2("武当",40),
      Tuple2("峨眉",50),
      Tuple2("武当",60)
    );
    val namesRDD=sc.parallelize(names);
    namesRDD.reduceByKey(_+_)
      .foreach(result => println(result._1 + " 得分："+result._2))
  }


  def groupByKey(){
    val conf=new SparkConf().setMaster("local").setAppName("groupByKey")
    val sc=new SparkContext(conf);
    val names=Array(
      Tuple2("峨眉","周芷若"),
      Tuple2("武当","张三丰"),
      Tuple2("峨眉","灭绝师太"),
      Tuple2("武当","张无忌")
    );
    val namesRDD=sc.parallelize(names, 1);
    namesRDD.groupByKey(1).foreach(result =>{
      println("门派："+result._1);
      result._2.foreach { name => println(name) }
      println("====================");

    })


  }


  def flatMap(){
    val conf=new SparkConf().setMaster("local").setAppName("flatMap")
    val sc=new SparkContext(conf);
    val word=Array("you	jump","i	jump");
    val wordRDD=sc.parallelize(word);
    wordRDD.flatMap { x => x.split("\t") }
      .foreach { x => println(x)}
  }


  def map(){
    val conf=new SparkConf().setMaster("local").setAppName("map")
    val sc=new SparkContext(conf);
    val names=Array("张无忌","赵敏","周芷若");
    val namesRDD=sc.parallelize(names);
    namesRDD.map { name => "hello  "+name }
      .foreach { name => println(name) }
  }


  def filter(){
    //sc
    val conf=new SparkConf().setMaster("local").setAppName("filter")
    val sc=new SparkContext(conf);
    val number=Array(1,2,3,4,5,6,7);
    val numberRDD= sc.parallelize(number, 1);
    numberRDD.filter { num => num % 2 == 0 }
      .foreach { x => println(x) }
  }

  def main(args: Array[String]): Unit = {

  }

}
