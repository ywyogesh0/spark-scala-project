package com.sparkworkshop.advanced

import java.util.regex.Pattern

import com.sparkworkshop.sparkstreaming.constants.StreamingConstants._
import com.sparkworkshop.sparkstreaming.utilities.StreamingUtility._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._

import scala.util.Try

object ApacheLogsIPSessionizer {

  def main(args: Array[String]): Unit = {

    // set log level
    setLogLevel()

    // retrieve log pattern
    val pattern: Pattern = retrieveApacheLogPattern()

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("ApacheLogsIPSessionizer")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val stateSpec = StateSpec.function(trackStateSpec _).timeout(Minutes(5))

    val logsDStream = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    val logsSessionDataDStream = logsDStream.map(log => {

      val matcher = pattern.matcher(log)
      if (matcher.matches()) {

        val requestArray = matcher.group(5).split(" ")

        val ip = matcher.group(1)
        val url = Try(requestArray(1)) getOrElse "[ERROR]"

        (ip, url)
      } else {
        ("[ERROR]", "[ERROR]")
      }
    })

    val stateSpecDStream = logsSessionDataDStream.mapWithState(stateSpec)
    val stateDStreamSnapshot = stateSpecDStream.stateSnapshots()

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import sparkSession.implicits._
    stateDStreamSnapshot.foreachRDD(rdd => {
      rdd.map(sessionData => (sessionData._1, sessionData._2.sessionLength, sessionData._2.urls))
        .toDF("ip", "sessionLength", "urls")
        .show()
    })

    ssc.checkpoint(SSC_CHECKPOINT_DIR_PATH)

    closeStreamingContext(ssc)
    stopSparkSession(sparkSession)
  }

  def trackStateSpec(batchTime: Time, ip: String, url: Option[String],
                     state: State[SessionData]): Option[(String, SessionData)] = {

    val previousSessionData = state.getOption().getOrElse(SessionData(0, List()))
    val newState = SessionData(previousSessionData.sessionLength + 1L,
      (previousSessionData.urls :+ url.getOrElse("[ERROR]")).take(10))

    state.update(newState)
    Some((ip, newState))
  }

  /** Custom state data type to track session data */
  case class SessionData(sessionLength: Long, urls: List[String])

}
