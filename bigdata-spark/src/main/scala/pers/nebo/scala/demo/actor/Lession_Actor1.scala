package pers.nebo.scala.demo.actor

import scala.actors.Actor

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/11/21
  * @ des :
  */

class MyActor extends  Actor{
  override def act(): Unit = {

    receive{
      case s:String=>{println(s"types is String value $s")}
      case _=>{println("no match ...")}
    }
  }
}
object Lession_Actor1 {
  def main(args: Array[String]): Unit = {
    val actor=new MyActor
    actor.start()
    actor !"hello world"
  }

}
