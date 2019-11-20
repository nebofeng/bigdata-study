package pers.nebo.scala.demo

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/11/20
  * @ des :
  */

/**
  *
  * 隐式转换函数是使用关键字implicit修饰的方法。当Scala运行时，假设如果A类型变量调用了method()这个方法，
  * 发现A类型的变量没有method()方法，而B类型有此method()方法，会在作用域中寻找有没有隐式转换函数将A类型转换成B类型，
  * 如果有隐式转换函数，那么A类型就可以调用method()这个方法。
  * 隐式转换函数注意：隐式转换函数只与函数的参数类型和返回类型有关，
  * 与函数名称无关，所以作用域内不能有相同的参数类型和返回类型的不同名称隐式转换函数。
  *
  */

class Animal(name:String){
  def canFly(): Unit ={

    println(s"$name  can fly ...")
  }

}

class Rabbit(xname:String){
  val name=xname
}
object Lession_ImplicitTrans2 {

  implicit def RabbitToAnimal(r:Rabbit):Animal={
    new Animal(r.name)
  }
  // 看到有隐式转化函数 将rabbit 当做参数，返回的参数里面有canfly函数
  def main(args: Array[String]): Unit = {
    val rabbit=new Rabbit("rabbit")
  }


}
