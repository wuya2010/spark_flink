package scala.streaming_source

//import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf

import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author kylinWang
  * @data 2020/6/30 22:36
  *
  */
object spark_kafka {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafka")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    //kafka 基本参数
    val brokers = "haodop105:9092,haodop106:9092,haodop107:9092"
    val topic = "first"
    val kafkaParams = Map[String,String]()

    val ds1 = null // getKafka1(ssc,kafkaParams,topic)
    println(ds1)

    ssc.start()
    ssc.awaitTermination()
  }

//  //方法一： 建立 kafka 连接
//  def getKafka1(ssc:StreamingContext, kafkaParams:Map[String,String],topic:String)={
//
//    //必须指定 createDirectStream 类型 , 否则返回值类型不确定
//    val sourceDstream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
//      ssc, kafkaParams, Set(topic))
//
//    sourceDstream
//  }




}
