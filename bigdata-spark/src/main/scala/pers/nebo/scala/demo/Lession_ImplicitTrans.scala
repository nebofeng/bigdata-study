package pers.nebo.scala.demo

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/11/20
  * @ des :
  */
/**
  * 隐式值 指的是定义参数时前面加上implicit 参数
  * 隐式参数是由 implicit 修饰【必须使用柯里化函数的方式，将隐式参数写在后面的括号中】
  * 隐式转换的作用是： 当调用函数的时候，不必手动传入方法中的隐式参数 Scala 会自动在作用域范围内寻找隐式值自动传入
  *
  */
object Lession_ImplicitTrans {
//  def sayName(implicit name:String): Unit ={
//    println(s"$name is a student...")
//  }
def sayName(age:Int)(implicit name:String): Unit ={
  println(s"$name is a student...")
}
  def main(args: Array[String]): Unit = {
    implicit val name:String="zs"
    //隐式值在同一个作用域只能定义一个
    sayName(1)
  }
}
