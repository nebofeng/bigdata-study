package pers.nebo.scala.utils

import java.text.SimpleDateFormat

object TimeUtils {
  def main(args: Array[String]): Unit = {

  }

  def tranTimeToLong1(tm:String) :Long={
    val fm = new SimpleDateFormat("yyyyMMddhhmmss")
    val dt = fm.parse(tm)
    val aa = fm.format(dt)
    val tim: Long = dt.getTime()
    tim
  }

  def tranTimeToLong(tm:String) :Long={
    val fm = new SimpleDateFormat("yyyyMMddhhmmssSSS")
    val dt = fm.parse(tm)
    val aa = fm.format(dt)
    val tim: Long = dt.getTime()
    tim
  }




}
