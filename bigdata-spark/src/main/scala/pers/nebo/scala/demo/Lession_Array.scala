package pers.nebo.scala.demo

import scala.collection.mutable.ArrayBuffer

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/11/19
  * @ des :
  */
object Lession_Array {

  def main(args: Array[String]): Unit = {

    val arr =ArrayBuffer[Int](1,2,3)
    arr.+=(4) //往后面加
    arr.+=:(100) //往前面加
    arr.append(7,8,9)
    arr.foreach(println)

    //    import scala.collection.mutable.ArrayBuffer




    //   val arr = Array[String]("a","b","c","d")
    //   val arr1 = Array[String]("e","f","g")
    //
    //    val array: Array[String] = Array.fill(5)("hello")
    //    array.foreach(println)

    //    val arrays: Array[String] = Array.concat(arr,arr1)
    //    arrays.foreach(println)



    //二维数组输出
    //   val array = new Array[Array[Int]](3)
    //    array(0) = Array[Int](1,2,3)
    //    array(1) = Array[Int](4,5,6)
    //    array(2) = Array[Int](7,8,9)



    //    array.foreach(arr=>{arr.foreach(println)})

    //    for (arr<-array;elem<-arr){
    //        println(elem)
    //    }



    //    val arr1 = new Array[Int](3)
    //    arr1(0) = 100
    //    arr1(1) = 200
    //    arr1(2) = 300
    //    arr1.foreach(println)

    //    for(elem<-arr){
    //      println(elem)
    //    }
    //    arr.foreach(println)
  }

}
