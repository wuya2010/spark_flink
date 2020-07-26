package src.main.scala.stream_project.bean

import java.sql.Timestamp

case class AdsInfo(ts: Long,  // 数字型的时间戳
                   timestamp: Timestamp, // 时间戳类型的时间戳
                   dayString: String,  // 2019-09-25
                   hmString: String,  // 10:20
                   area: String,
                   city: String,
                   userId: String,
                   adsId: String)