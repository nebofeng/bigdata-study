package pers.nebo.sparkstreaming.streamingonkafka

import java.util

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

import scala.collection.mutable

object RedisClient {
  val redisHost = "node4"
  val redisPort = 6379
  val redisTimeout = 30000
  /**
    * JedisPool是一个连接池，既可以保证线程安全，又可以保证了较高的效率。
    */
  lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout)


//  def getOffSetFromRedis(db:Int,topic:String)  ={
//    val jedis = RedisClient.pool.getResource
//    jedis.select(db)
//    val result: util.Map[String, String] = jedis.hgetAll(topic)
//    RedisClient.pool.returnResource(jedis)
//    if(result.size()==0){
//      result.put("0","0")
//      result.put("1","0")
//      result.put("2","0")
//    }
//
//    import scala.collection.JavaConversions.mapAsScalaMap
//    val offsetMap: scala.collection.mutable.Map[String, String] = result
//    offsetMap
//  }

//  def main(args: Array[String]): Unit = {
//    val stringToString: mutable.Map[String, String] = getOffSetFromRedis(3,"mytest")
//    println(stringToString)
//  }
}
