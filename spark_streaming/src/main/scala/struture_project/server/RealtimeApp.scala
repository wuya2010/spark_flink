package src.main.scala.struture_project.server

import java.text.SimpleDateFormat

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.{SparkConf, SparkContext}


object RealtimeApp {
  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkConf 对象
    val conf: SparkConf = new SparkConf()
      .setAppName("RealTimeApp")
      .setMaster("local[*]")
    // 2. 创建 SparkContext 对象
    val sc = new SparkContext(conf)

    // 3. 创建 StreamingContext
    val ssc = new StreamingContext(sc, Seconds(5))

    ssc.checkpoint("./ck1")
    ssc.sparkContext.setLogLevel("warn")


    // 4. 得到 DStream
    val recordDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getDStream(ssc, "ads_log")

    // 从 kafka 读取数据, 为了方便后续处理, 封装数据到 AdsInfo 样例类中
    val dayStringFormatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val hmStringFormatter: SimpleDateFormat = new SimpleDateFormat("HH:mm")


    // 5. 为了方便后面的计算, 把消费到的字符串封装到对象中
    val adsInfoDStream: DStream[AdsInfo] = recordDStream.map {
      record =>
        val split: Array[String] = record.value.split(",")
        val date: Date = new Date(split(0).toLong)
        AdsInfo(
          split(0).toLong,
          new Timestamp(split(0).toLong),
          dayStringFormatter.format(date),
          hmStringFormatter.format(date),
          split(1),
          split(2),
          split(3),
          split(4))

    }
    // 需求 1:  每天每地区广告 top3 广告
    AreaAdsClickTop3App.statAreaClickTop3(adsInfoDStream)
    ssc.start()
    ssc.awaitTermination()
  }
}