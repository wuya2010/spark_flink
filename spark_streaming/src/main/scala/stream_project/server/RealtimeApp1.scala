package src.main.scala.stream_project.server

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.SparkSession
import src.main.scala.stream_project.bean.AdsInfo

/**
  * Author kylin
  * Date 2019-09-27 09:12
  */
object RealtimeApp1 {
    def main(args: Array[String]): Unit = {
        // 1. 先从kafka消费数据

      //structstreaming
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("RealtimeApp1")
            .getOrCreate()

        //记得导包呢  ==>实现df ==》ds
        import spark.implicits._


      //java中的方法 ：  import java.text.SimpleDateFormat
      //以将日期格式化为指定字符串和将字符串解析成日期
        val dayFormatter = new SimpleDateFormat("yyyy-MM-dd")
        val hmFormatter = new SimpleDateFormat("HH:mm")


      //输入流
        val adsInfoDS= spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "hadoop201:9092,hadoop202:9092,hadoop203:9092")
            .option("subscribe", "ads_log")
            .load

            .select("value")
            .as[String]
            .map(line => {
       //对数据进行分隔  1569588591712,华中,杭州,104,3   根据，分隔
       val arr: Array[String] = line.split(",")


       //时间戳转成 toLong  导包： import java.util.Date
       val date: Date = new Date(arr(0).toLong)

       //封装到样例类中
       AdsInfo(
           arr(0).toLong,
           //导包： import java.sql.Timestamp
           new Timestamp(arr(0).toLong),
           dayFormatter.format(date),
           hmFormatter.format(date),
           arr(1),
           arr(2),
           arr(3),
           arr(4)
       )
   })

//测试: 数据是否拉通
/*    adsInfoDs.writeStream
   .format("console")
   .outputMode("update")
   .start
   .awaitTermination()

   //  可以正常输出
   */




//2. 需求1: 黑名单   调用方法
//        val filteredAdsInfoDS: Dataset[AdsInfo] = BlackListApp.statBlackList(spark, adsInfoDS)

// 需求2:


}
}
