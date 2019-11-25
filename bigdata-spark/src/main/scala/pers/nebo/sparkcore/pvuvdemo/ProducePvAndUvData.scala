package pers.nebo.sparkcore.pvuvdemo

import java.io.{File, FileOutputStream, OutputStreamWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.util.{Date, Random}

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/11/25
  * @ des : 创建pv uv data
  */
object ProducePvAndUvData {
  //ip
  val IP = 223
  //地址
  val ADDRESS = Array("北京", "天津", "上海", "重庆", "河北", "辽宁","山西",
    "吉林", "江苏", "浙江", "黑龙江", "安徽", "福建", "江西",
    "山东", "河南", "湖北", "湖南", "广东", "海南", "四川",
    "贵州", "云南", "山西", "甘肃", "青海", "台湾", "内蒙",
    "广西", "西藏", "宁夏", "新疆", "香港", "澳门")
  //日期
  val DATE = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
  //timestamp
  val TIMESTAMP = 0L
  //userid
  val USERID = 0L
  //网站
  val WEBSITE = Array("www.baidu.com", "www.taobao.com", "www.dangdang.com", "www.jd.com", "www.suning.com", "www.mi.com", "www.gome.com.cn")
  //行为
  val ACTION = Array("Regist", "Comment", "View", "Login", "Buy", "Click", "Logout")

  def main(args: Array[String]): Unit = {
    val pathFileName = "./pvuvdata"
    //创建文件
    val createFile = CreateFile(pathFileName)

    //向文件中写入数据 需要的对象
    val file = new File(pathFileName)
    val fos = new FileOutputStream(file, true)
    val osw = new OutputStreamWriter(fos, "UTF-8")
    val pw = new PrintWriter(osw)

    if (createFile) {
      var i = 0
      //产生5万+数据
      while (i < 50000){ //模拟一个ip
        val random = new Random()
        val ip = random.nextInt(IP) + "." + random.nextInt(IP) + "." + random.nextInt(IP) + "." + random.nextInt(IP)
        //模拟地址
        val address = ADDRESS(random.nextInt(34))
        //模拟日期
        val date = DATE
        //模拟userid
        val userid = Math.abs(random.nextLong)
        /**
          * 这里的while模拟是同一个用户不同时间点对不同网站的操作
          */
        var j = 0
        var timestamp = 0L
        var webSite = "未知网站"
        var action = "未知行为"
        val flag = random.nextInt(5) | 1
        while (j < flag) { //					Threads.sleep(5);
          //模拟timestamp
          timestamp = new Date().getTime()
          //模拟网站
          webSite = WEBSITE(random.nextInt(7))
          //模拟行为
          action = ACTION(random.nextInt(6))
          j += 1
          /**
            * 拼装
            */
          val content = ip + "\t" + address + "\t" + date + "\t" + timestamp + "\t" + userid + "\t" + webSite + "\t" + action
          System.out.println(content)
          //向文件中写入数据
          pw.write(content + "\n")
        }
        i += 1
      }

      //注意关闭的先后顺序，先打开的后关闭，后打开的先关闭
      pw.close()
      osw.close()
      fos.close()

    }
  }

  /**
    * 创建文件
    */
  def CreateFile(pathFileName: String): Boolean = {
    val file = new File(pathFileName)
    if (file.exists) file.delete
    val createNewFile = file.createNewFile()
    System.out.println("create file " + pathFileName + " success!")
    createNewFile
  }
}
