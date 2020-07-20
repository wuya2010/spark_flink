package scala.HotAnalysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


/**
  * @author kylinWang
  * @data 2020/7/12 22:37
  *
  */
object Hots {
  def main(args: Array[String]): Unit = {

      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
      env.setParallelism(1)

      val inputStream = env.socketTextStream("hadoop105",7777)
        .map(data => {
          val dataArray = data.split(",")
          UserBehavior(dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
        })
        .assignAscendingTimestamps(_.timestamp * 1000L)

    val aggStream = inputStream.filter(_.behavior == "pv")
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAgg(), new windowCountResult())


    val processStream = aggStream
      .keyBy(_.windowEnd).process(new getTop(3))

    processStream.print("process flink")

    env.execute()
  }
}

//输入数据
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)
//中间样例
case class ItemViewCount(itemId: Long, windowEnd: Long,count: Long)


//一加一
class CountAgg() extends  AggregateFunction[UserBehavior ,Long , Long]{
  override def createAccumulator(): Long = ???

  override def add(value: UserBehavior, accumulator: Long): Long = ???

  override def getResult(accumulator: Long): Long = ???

  override def merge(a: Long, b: Long): Long = ???
}

//平均值
class MyAverageAgg() extends  AggregateFunction[Long, (Long, Int), Double]{
  override def createAccumulator(): (Long, Int) = ???

  override def add(value: Long, accumulator: (Long, Int)): (Long, Int) = ???

  override def getResult(accumulator: (Long, Int)): Double = ???

  override def merge(a: (Long, Int), b: (Long, Int)): (Long, Int) = ???
}


//自定义窗口
class windowCountResult() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {

  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = ???


}


//自定义processFunction
class getTop(topNum: Int) extends  KeyedProcessFunction[Long, ItemViewCount, String] {

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = ???

}

