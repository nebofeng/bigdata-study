package pers.nebo.scala.demo.actor

import scala.actors.Actor

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/11/22
  * @ des :
  */


case class Message(actor: Actor,msg:String)

class MyActor1 extends  Actor{
  override def act(): Unit = {
    while (true){
      receive{
        case msg:Message=>{
          if("hello".equals(msg.msg)){
            println(s"${msg.msg}")
            msg.actor ! "hi"
          }else if("Could we have a date?".equals(msg.msg)){
             println(" ok" )
          }

        }
        case _=>{println("no match ...")}
      }
    }

  }
}


class MyActor2(actor: Actor) extends  Actor{

  actor ! Message(this,"hello")
  override def act(): Unit = {
    while (true){
      receive{
        case s:String=>{
          println(s"types is String value $s")
          actor! Message(this,"Could we have a date?")
        }
        case _=>{println("no match ...")}
      }
    }

  }
}



object Lession_Actor2 {
  def main(args: Array[String]): Unit = {
    val actor1=new MyActor1()
    val actor2=new MyActor2(actor1)

    actor1.start()
    actor2.start()
  }

}
