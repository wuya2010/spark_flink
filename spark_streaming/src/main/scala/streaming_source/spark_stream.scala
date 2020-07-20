package scala.streaming_source

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author kylinWang
  * @data 2020/6/30 22:12
  *     sockest 输入数据流： transform 具体作用？？
  */
object spark_stream {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("nc_test").setMaster("local[*]")
    val ssc:StreamingContext =  new StreamingContext(conf,Seconds(10))
    ssc.sparkContext.setLogLevel("WARN")

    val socketStreaming: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop105",7777)

    //测试1: 数据不累加
    val ds1 = test_transform(socketStreaming)
    ds1.print()  //结果： (tian,3)

    //测试2：数据叠加
//    ssc.checkpoint("./chk1")
//    val ds2 = test_updateStateByKey(socketStreaming)
//    ds2.print()

    ssc.start() //执行流
    ssc.awaitTermination()  //等待线程结束
  }


  // 使用 transform  算子
  def test_transform(socketStreaming:ReceiverInputDStream[String]) = {

    val getStreaming = socketStreaming.transform(rdd => {
      rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    })

   getStreaming
  }

    //使用 updateStateByKey  算子
  def test_updateStateByKey(socketStreaming:ReceiverInputDStream[String]) ={

        val getStreaming2 = socketStreaming.flatMap(_.split(" "))
          .map((_,1))
          .updateStateByKey((Seq:Seq[Int],opt:Option[Int])=>{  //能记住状态
          Some(Seq.sum + opt.getOrElse(0))
        })

        getStreaming2
  }


}
