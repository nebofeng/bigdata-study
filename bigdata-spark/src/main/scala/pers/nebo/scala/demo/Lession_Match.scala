package pers.nebo.scala.demo

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/11/20
  * @ des :
  */

/**
  * Match 模式匹配
  * 1.case _ 什么都匹配不上匹配，放在最后
  * 2.match 可以匹配值还可以匹配类型
  * 3.匹配过程中会有数值的转换
  * 4.从上往下匹配，匹配上之后会自动终止
  * 5.模式匹配外部的“{..}”可以省略
  */
object Lession_Match {
  def main(args: Array[String]): Unit = {

  }

  def MatchTest(o:Any): Unit ={
    o match {
      case i:Int=>println(s"type is Int ,value = $i")
      case 1=>println("value is 1")
      case d:Double=>println(s"type is Double ,value = $d")
      case s:String=>println(s"type is String ,value = $s")
      case 'a'=>println("value is c")
      case _=>{println("no match...")}
    }
  }
}
