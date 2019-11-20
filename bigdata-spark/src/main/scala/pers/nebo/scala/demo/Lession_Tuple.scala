package pers.nebo.scala.demo

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/11/19
  * @ des : 元组
  */
object Lession_Tuple {
  def main(args: Array[String]): Unit = {
    //元组可以new 可以不new  甚至可以直接 （）
    val tuple1: Tuple1[String] = new Tuple1("hello")
    val tuple2: (String, Int) = new Tuple2("a", 100)
    val tuple3: (Int, Boolean, Char) = new Tuple3(1,true,'C')
    val tuple4: (Int, Double, String, Boolean) = Tuple4(1,3.4,"abc",false)
    val tuple6: (Int, Int, Int, Int, Int, String) = (1,2,3,4,5,"abc")
    //元组合最多支持22个元素
    val tuple22 = (1,2,3,4,5,6,7,8,9,10,11,12,"abc",14,15,16,17,18,19,20,21,22)
    // swap方法只针对二元组才有这个方法。
    println(tuple2.swap)
    //    val value: String = tuple22._13

    //    遍历元组
    //    println(tuple4)
    //    val iter: Iterator[Any] = tuple6.productIterator
    //    iter.foreach(println)


    //    while(iter.hasNext){
    //        println(iter.next())
    //    }


    //     这样是不行的
    //    for(elem <- tuple6){
    //      println(elem)
    //    }

    //    println(value)
  }
}
