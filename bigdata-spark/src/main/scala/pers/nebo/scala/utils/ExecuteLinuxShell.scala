package pers.nebo.scala.utils

import java.io.{BufferedReader, InputStreamReader}

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/12/26
  * @ des :
  */
object ExecuteLinuxShell {
  def main(args: Array[String]): Unit = {

    val cmd =" ls " + args(0)

    val process= Runtime.getRuntime().exec(cmd)
    /**
      * 可执行程序的输出可能会比较多，而运行窗口的输出缓冲区有限，会造成waitFor一直阻塞。
      * 解决的办法是，利用Java提供的Process类提供的getInputStream,getErrorStream方法
      * 让Java虚拟机截获被调用程序的标准输出、错误输出，在waitfor()命令之前读掉输出缓冲区中的内容。
      */
    val bufferReader = new BufferedReader(new InputStreamReader(process.getInputStream))
    var flag=bufferReader.readLine()
    while(flag!=null){
      println(flag)
      flag=bufferReader.readLine()
    }

    bufferReader.close()




  }

}
