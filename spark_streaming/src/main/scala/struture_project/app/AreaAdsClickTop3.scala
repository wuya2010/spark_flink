package src.main.scala.struture_project.app

import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods

/**
  * Author kylin
  * Date 2019-09-27 14:31
  */


//field: 华北      value:  {"2":1200，...}
object AreaAdsClickTop3 {

    def statAearAdsClick(adsInfoDStream: DStream[AdsInfo]) = {


        val dayAreaAndAdsCountDStream: DStream[((String, String), (String, Int))] = adsInfoDStream

            .map(adsInfo => ((adsInfo.dayString, adsInfo.area, adsInfo.adsId), 1)) // ((day, area, adsId), 1)

            .updateStateByKey((seq: Seq[Int], option: Option[Int]) => { // ((day, area, adsId), 1000)

            Some(seq.sum + option.getOrElse(0))
        })
            .map {
                case ((day, area, adsId), count) => ((day, area), (adsId, count))
            }


        
        // 分组, 排序, top3
        val dayAreaAdsClickCountTop3: DStream[(String, String, List[(String, Int)])] = dayAreaAndAdsCountDStream.groupByKey
            .map {
                case ((day, area), adsIdCountItable) => {
                    (day, area, adsIdCountItable.toList.sortBy(-_._2).take(3))
                }
            }


        // 写到redis中
        dayAreaAdsClickCountTop3.foreachRDD(rdd => {
            
            val client: Jedis = RedisUtil.getJedisClient
            
            // 数据量已经很小, 所以可以在驱动端统一写出
            val arr: Array[(String, String, List[(String, Int)])] = rdd.collect
            /*arr.foreach {
                case (day, area, adsCountList) => {
                    
                    import org.json4s.JsonDSL._
                    client.hset(s"area:ads:top3:$day", area, JsonMethods.compact(JsonMethods.render(adsCountList)))
                }
            }*/
            
            import org.json4s.JsonDSL._
            val resultMap = arr.map{
                case (day, area, list) => (area, JsonMethods.compact(JsonMethods.render(list)))
            }.toMap
            client.hmset(s"area:ads:top3:${arr(0)._1}", resultMap)  // 批量写法
            
            client.close()
        })
    }
}

/*
统计每天每地区每广告的点击量
((dayString, area, adsId), 1)




 */