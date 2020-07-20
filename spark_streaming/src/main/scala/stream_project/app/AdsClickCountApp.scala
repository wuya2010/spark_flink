package src.main.scala.stream_project.app

import com.atguigu.realtime.bean.AdsInfo
import com.atguigu.realtime.util.RedisUtil
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import redis.clients.jedis.Jedis
/**
  * Author lzc
  * Date 2019-09-27 11:29
  */
object AdsClickCountApp {

    //根据第一步获得的过滤数据求解
    def statClickCount(spark: SparkSession, filteredAdsInfoDS: Dataset[AdsInfo]) = {

        val resultDF: DataFrame = filteredAdsInfoDS
          //对每天每地区进行分组  ： 字段得划分要理解清楚
            .groupBy("dayString","area", "city", "adsId")
            .count()

        //写入流
        resultDF.writeStream
            .outputMode("complete")
          //foreachBatch:用法
            .foreachBatch((df, bachId) => {
               //进行了缓存
                df.persist()
                if (df.count() > 0) {
                    // 读取黑名单, 删除黑名单的计数记录
                    println("df count > 0")

                    //分区： 按分区来划分
                    df.foreachPartition(rowIt => {

                        //建立连接
                        val client: Jedis = RedisUtil.getJedisClient
                        //导入2个string类型  变量类型： var
                        var dayString: String = ""
                        //stirng
                        val hashValue: Map[String, String] = rowIt.map(row => {
                            dayString = row.getString(0)
                            val area = row.getString(1)
                            val city = row.getString(2)
                            val adsId = row.getString(3)
                            //这里多一个列出来： count
                            val count: Long = row.getLong(4)
                            //count转换成toStrinbg
                            (s"$area:$city:$adsId", count.toString)
                        }).toMap

                            //早点导进去就对了
                        import scala.collection.JavaConversions._

                                //做一个判断: nonEmpty: 非空
                        if (hashValue.nonEmpty)  //

                            //往redis中插入值  ： 传入2个string类型得值

                            //hashValue: $area:$city:$adsId", count.toString

                            //最后的结果： date:area:city:ads:"2019-08-19"    field(华北：北京：5) ， 1000
                           //将多个数据封装为一个map
                            client.hmset(s"date:area:city:ads:$dayString", hashValue)
                        client.close()
                    })
                }
                df.unpersist()
            })
          //报导import org.apache.spark.sql.streaming.Trigger
            .trigger(Trigger.ProcessingTime(10000))
            .start
            .awaitTermination()

    }
}
