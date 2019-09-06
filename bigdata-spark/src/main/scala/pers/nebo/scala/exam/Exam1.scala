package pers.nebo.scala.exam

/***
  * @author  fnb
  * @email  nebofeng@gmail.com
  * @date 2018/12/16
  * @des :
  */
object Exam1 {


  def main(args: Array[String]): Unit = {
//    mothed1("fghjkl")
//    method2(2)
    method3
  }
//  1.现在有一个字符串”fghjkl”，请使用scala语言获取字符串的首字符和尾字符？
  def  mothed1(s:String):Unit={
    println(s.charAt(0))
    println(s.charAt(s.length-1))

  }

//  一个数字如果为正数，则它的signum为1；如果是负数，则signum为-1；如果是0，则signum为0.
//    使用scala语言编写一个函数来计算这个值。
  def  method2(s:Int): Int ={
        if(s>0){
          1
        }else if(s<0){
          -1
        }else{
          0
        }
  }
//  3.针对下列Java循环代码翻译成scala版本的循环代码：for(int i=10;i >=0;i--) System.out.println(i);
  def  method3(): Unit ={
      //var a = 10
     for( a <- 10 to  0 by -1 ){
          println(a)
     }

  }

}
