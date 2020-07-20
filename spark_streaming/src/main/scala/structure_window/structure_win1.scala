package main.scala.structure_window

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
/**
  * @author kylinWang
  * @data 2020/7/16 0:19
  *
  */
object structure_win1 {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Window1")
      .getOrCreate()

    import spark.implicits._

    val dataSrouce = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",4444)
      .load()
      .as[String]

    val datadf = dataSrouce .map(line => {
      val split: Array[String] = line.split(",")
      (split(0), split(1))
    }).toDF("ts", "word")

    //根据窗口聚合
    val aggdf = datadf.groupBy(
      window(col(""),"10 minutes", "3 minutes"),col("word")
    ).count()

    aggdf.writeStream
      .format("console")
      .outputMode("complete")
//      .trigger(Trigger.ProcessingTime(2000))  // 这个引入不了
      .option("truncate", false)
      .start
      .awaitTermination()




  }

}
