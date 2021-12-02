package src.main.scala

import java.sql.Timestamp

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
/**
  * @author kylinWang
  * @data 2020/7/16 0:19
  *
  */
object renameWindow {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("WordCountWatermark1")
      .getOrCreate()

    //输入数据
    val lines = spark.readStream
      .format("socket")
      .option("host", "192.168.25.229")
      .option("port", 9999)
      .load

    import spark.implicits._
    val datadf = lines.as[String].flatMap(row => {
      val split = row.split(",")
      split(1).split(" ").map((_, Timestamp.valueOf(split(0)))) //拼接
    }).toDF("word", "timestamp")//命名


    //加入watermark
    val ret_ds= datadf
      // 添加watermark, 参数 1: event-time 所在列的列名 参数 2: 延迟时间的上限.
      .withWatermark("timestamp","2 minutes")
      .groupBy(
        //根据窗口 与 word 进行 group by 操作
        window(col("timestamp"),"10 minutes", "2 minutes"),col("word")
      ).count()


    val query = ret_ds.writeStream
      .outputMode("append")
//      .trigger(Trigger.ProcessingTime(2000))
      .format("console")
      .option("truncate", "false")
      .start

      query.awaitTermination()


  }
}
