package pers.nebo.scala.demo

import scala.collection.mutable

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/11/19
  * @ des :
  */
object Lession_Map {
  def main(args: Array[String]): Unit = {
    import scala.collection.mutable.Map
    val map = Map[String,Int]()
    map.put("a",100)
    map.put("b",200)
    //    map.foreach(println)
    val result: mutable.Map[String, Int] = map.filter(tp => {
      val key = tp._1
      val value = tp._2
      value == 200
    })
    result.foreach(println)

    //    val map1 = Map[String,Int](("a",1),("b",2),("c",3),("d",4))
    //    val map2 = Map[String,Int](("a",100),("b",2),("c",300),("e",500))
    // 合并map 不加冒号是 2合并到1 里面，加上冒号是1 合并到2 里面。  被合并的一方同样的key value会被覆盖
    // 冒号代表在前面 ，其他容器运算时也是这个规律
    //    val result: Map[String, Int] = map1.++:(map2)
    //    result.foreach(println)


    //
    //    val map = Map[String,Int]("a"->100,"b"->200,("c",300),("c",400))
    // 遍历所有values
    //    val values: Iterable[Int] = map.values
    //    values.foreach(println)


    // 遍历keys
    //    val keys: Iterable[String] = map.keys
    //    keys.foreach(key=>{
    //      val value = map.get(key).get
    //      println(s"key = $key ,value = $value")
    //    })

    //    val value = map.get("aa").getOrElse("no value")
    //    println(value)


    //    map.foreach(println)

    //    println(map)
    //    for(elem <- map){
    //      println(elem)
    //    }
  }
}
