package pers.nebo.scala.demo

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/11/21
  * @ des :
  */
/**
  *
  * 使用implicit关键字修饰的类就是隐式类。若一个变量A没有某些方法或者某些变量时，
  * 而这个变量A可以调用某些方法或者某些变量时，可以定义一个隐式类，隐式类中定义这些方法或者变量，隐式类中传入A即可。
  * 隐式类注意：
  * 1).隐式类必须定义在类，包对象，伴生对象中。
  * 2).隐式类的构造必须只有一个参数，同一个类，包对象，伴生对象中不能出现同类型构造的隐式类。
  */
class  Rabbit1(xname:String){
  val name=xname
}
object Lession_ImplicitTrans3 {
  implicit class Animal1(r:Rabbit1){
    def showName(){
      println(s"${r.name} is rabbit ...")
    }
  }

  def main(args: Array[String]): Unit = {
    val rabbit1=new Rabbit1("Rabbit1")
    rabbit1.showName()
  }

}
