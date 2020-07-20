package scala.streaming_source

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author kylinWang
  * @data 2020/6/30 22:36
  *   spark窗口的使用
  */
object spark_windows {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount2")
    // 参数2-duration（ 周期）： 微批处理，这个是微批处理的时间
    val ssc = new StreamingContext(conf, Seconds(4))
  }



  //窗口一: reduceByKeyAndWindow
  def getWindow_1(conf:SparkConf, ssc:StreamingContext)={

    val sourceDStream = ssc.socketTextStream("localhost", 7777)

    val window_ds: DStream[(String, Int)] = sourceDStream.flatMap(_.split(" ")).map((_,1))
      .reduceByKeyAndWindow((_:Int) + (_:Int), Seconds(12),Seconds(8))

    window_ds.print()

    // 5. 启动 teamingContext： 因为是实时任务，一旦启动就不要停
    ssc.start()
    //6. 阻止当前线程退出 ： 让主线程一直在这里等着，类似sleep ： 等待ssc结束后，才退出
    ssc.awaitTermination()
  }

  //窗口2: 保持状态
  def getWindow_2(conf:SparkConf, ssc:StreamingContext)= {
      //保持状态
      val sourceDStream = ssc.socketTextStream("localhost", 7777)

    val window_ds: DStream[(String, Int)] = sourceDStream.flatMap(_.split(" ")).map((_,1))
      // updateStateByKey ：Return a new "state" DStream 其中应用程序更新每个键的状态
      // 必须是键值对的形式
      //updateFunc: (Seq[V], Option[S]) => Option[S]
      //option: 可能有也可能没有   opt: Option[Int]实质上是上一个时间段的值
      //seq： 同一个key这一次vaule的序列
      .updateStateByKey[Int]((seq: Seq[Int], opt: Option[Int]) => Some(seq.sum + opt.getOrElse(0)))


    window_ds.print

    // 5. 启动 teamingContext： 因为是实时任务，一旦启动就不要停
    ssc.start()
    //6. 阻止当前线程退出 ： 让主线程一直在这里等着，类似sleep ： 等待ssc结束后，才退出
    ssc.awaitTermination()


  }
}
