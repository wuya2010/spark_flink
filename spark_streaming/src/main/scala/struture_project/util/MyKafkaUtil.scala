package src.main.scala.struture_project.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, LocationStrategies, _}

/**
  * Author lzc
  * Date 2019-09-27 14:13
  */
object MyKafkaUtil {

    //kafka: 基本配置
    val kafkaParams = Map[String, Object](

        "bootstrap.servers" -> "hadoop201:9092,hadoop202:9092,hadoop203:9092",

        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "bigdata",
        //可以使用这个配置，latest自动重置偏移量为最新的偏移量
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (true: java.lang.Boolean)
    )


    /*
    创建DStream，返回接收到的输入数据

    LocationStrategies：根据给定的主题和集群地址创建consumer

    LocationStrategies.PreferConsistent：持续的在所有Executor之间分配分区

    ConsumerStrategies：选择如何在Driver和Executor上创建和配置Kafka Consumer

    ConsumerStrategies.Subscribe：订阅一系列主题
    */
    def getKafkaStream(ssc:StreamingContext, topic: String): InputDStream[ConsumerRecord[String, String]] = {

        //kafka直连
        KafkaUtils.createDirectStream(

            ssc,

            //// 标配. 只要 kafka 和 spark 没有部署在一台设备就应该是这个参数
            LocationStrategies.PreferConsistent,
                //订阅
            ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParams)
        )
    }
    
}
