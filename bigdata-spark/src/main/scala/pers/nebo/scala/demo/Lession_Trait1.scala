package pers.nebo.scala.demo

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/11/20
  * @ des :
  */

trait Read{
  def read(name:String): Unit ={
    println(s"$name is read")
  }
}

trait Listen{
  def listen(name:String): Unit ={
    println(s"$name is listen")
  }
}

/**
  * 一个类继承多个trait时，第一个关键字使用 extends，之后使用with
  * trait 不可以传参
*/
class Human() extends  Read with  Listen {


}


object Lession_Trait1 {

  def main(args: Array[String]): Unit = {
    val h=new Human()
    h.read("xxx")
    h.listen("---")
  }


}
