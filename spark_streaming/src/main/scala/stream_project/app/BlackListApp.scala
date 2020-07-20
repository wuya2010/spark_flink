package src.main.scala.stream_project.app

import java.util

import com.atguigu.realtime.bean.AdsInfo
import com.atguigu.realtime.util.RedisUtil
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, ForeachWriter, Row, SparkSession}
import redis.clients.jedis.Jedis

/**
  * Author lzc
  * Date 2019-09-27 10:06
  */

/*
黑名单
 */

/*

测试：
adsInfoDS.createOrReplaceTempView("adsInfo")
spark.sql(
"""
  |select
  |dayString,
  |userId
  |from adsInfo
  |group by dayString,userId,adsId
  |having count(*) >=5
""".stripMargin)
.writeStream
.format("console")
.outputMode("complete")
.trigger(Trigger.ProcessingTime(2000))
.start()
.awaitTermination()
*/


object BlackListApp {

    //添加一个方法，在main方法种直接调用
    def statBlackList(spark: SparkSession, adsInfoDS: Dataset[AdsInfo]) = {

        import spark.implicits._

        // 1. 先过滤掉黑名单用户的点击记录   : 将不包含的去掉  过滤掉
        //利用分区减小连接数
        //将 迭代 器  转 为 list
        val filteredAdsInfoDS: Dataset[AdsInfo] = adsInfoDS.mapPartitions(it => {

            //: Iterator[AdsInfo]==>List[AdsInfo]
            val adsInfoList: List[AdsInfo] = it.toList

            //获取redis的连接： 拿到客户端
            val client: Jedis = RedisUtil.getJedisClient

            // 黑名单   :  去重   在list集合中：取adsInfoList(0)    //dayString: 样例类种的字段dayString: String,
            //把key给过来
            val blacList: util.Set[String] = client.smembers(s"blacklist:${adsInfoList(0).dayString}")
            // 过滤
            adsInfoList.filter(adsInfo => !blacList.contains(adsInfo.userId)).toIterator
        })




        // 2. 重新统计黑名单(已经进入黑名单的不会重新计算)
    //逐步完善，可以先写逻辑，再对逻辑进行完善
        adsInfoDS.createOrReplaceTempView("adsInfo")

//        filteredAdsInfoDS.createOrReplaceTempView("adsInfo")

        spark.sql(
            """
              |select
              | dayString,
              | userId
              |from adsInfo
              |group by dayString, userId, adsId
              |having count(*)>=100
            """.stripMargin)
            .writeStream
          .outputMode("update")
            .foreach(new ForeachWriter[Row] {
                    //哎这里建一个对象
                var client: Jedis = _

                // 建立连接
                override def open(partitionId: Long, epochId: Long): Boolean = {
                    client = RedisUtil.getJedisClient
                    client != null && client.isConnected
                }

                // 写出数据  row: value
                override def process(value: Row): Unit = {
                    val day = value.getString(0)
                    val userId = value.getString(1)

                    // sadd(key: String, members: String*) : 2个String类型
                    client.sadd(s"blacklist:$day", userId)
                }

                // 关闭连接
                override def close(errorOrNull: Throwable): Unit = {
                    if (client != null && client.isConnected) {
                        client.close()
                    }
                }
            })
            .trigger(Trigger.ProcessingTime(10000))
            .start()
    //.awaitTermination()   要加上这个才能循环


        //这里这个是为问题二得一个返回值，运行第一个这个不能有
        filteredAdsInfoDS

    }
}
