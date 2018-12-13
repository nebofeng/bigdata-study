package pers.nebo.scala.utils

object StringUtils {
  def main(args: Array[String]): Unit = {

  }
  //十进制根据需要转为制定位数的16进制字符串.
  def toHex(string:String ,t:Int): String ={
    val s=java.lang.Long.toHexString(string.toLong).toUpperCase
    var v = "0x";
    val num = t - s.length;
    var c=0
    for(c<- 1 to num){
      v=v+"0"
      +c;
    }
    v=v+s;
    v
  }

  //进制转换 16进制转为10进制
  def tranString(s:String): Int ={
    //如果是0x开头的,数字(字符串) 去掉0x之后使用Integer.par
    if(s.startsWith("0x")){
      Integer.parseInt(s.substring(2).toString,16)
    }else{
      Integer.parseInt(s.toString,16)
    }
  }

}
