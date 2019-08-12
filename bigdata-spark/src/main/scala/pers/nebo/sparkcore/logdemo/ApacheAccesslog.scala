package pers.nebo.sparkcore.logdemo

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/8/12
  * @ des :
  */

case class ApacheAccesslog(
                            ipAddress:String, // ip地址
                            clientIndentd:String, //标识符
                            userId:String ,//用户ID
                            dateTime:String ,//时间
                            method:String ,//请求方式
                            endPoint:String ,//目标地址
                            protocol:String ,//协议
                            responseCode:Int ,//网页请求响应类型
                            contenSize:Long   //内容长度

                          )

object ApacheAccesslog {
  def parseLog(log:String):ApacheAccesslog={
    val logArray= log.split("#");
    val url=logArray(4).split(" ")

    ApacheAccesslog(logArray(0),logArray(1),logArray(2),logArray(3),url(0),url(1),url(2),logArray(5).toInt,logArray(6).toLong);
  }

}
