package src.main.scala.struture_project.app

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds}
import org.json4s.jackson.JsonMethods

/**
  * Author kylin
  * Date 2019-09-27 15:31
  */


// 最近 1 小时广告点击量实时统计

//field: 广告id     value：{小时分钟：点击次数， 分钟数：点击次数}
object LastHourAdsClickApp {
    def statLastHourClick(adsInfoDStream: DStream[AdsInfo]) = {
        val adsInfoDStreamWithWindow: DStream[AdsInfo] = adsInfoDStream.window(Minutes(60), Seconds(5))
        
        // (adsId, List((10:21, 1000)))
        val adsAndHMCountDSteam: DStream[(String, List[(String, Int)])] = adsInfoDStreamWithWindow.map(adsInfo => ((adsInfo.adsId, adsInfo.hmString), 1)) // ((广告, 10:20), 1)
            .reduceByKey(_ + _)
            .map {
                case ((ads, hm), count) => (ads, (hm, count))
            }
            .groupByKey
            .map {
                case (ads, it) => (ads, it.toList.sortBy(_._1))  // 按照 hh:mm 升序排列
            }
        
        // 写入到redis
        adsAndHMCountDSteam.foreachRDD(rdd => {
            rdd.foreachPartition(it => {
                val client: Jedis = RedisUtil.getJedisClient
                
                it.foreach {
                    case (ads, hmCountList) =>
                        import org.json4s.JsonDSL._
                        val v = JsonMethods.compact(JsonMethods.render(hmCountList))
                        client.hset("last_hour_click", ads, v)
                }
                
                client.close()
            })
        })
    }
}
