package com.sparkworkshop.sparkstreaming.clickstream

import java.util.concurrent.atomic.AtomicLong
import java.util.regex.Pattern

import com.sparkworkshop.sparkstreaming.constants.StreamingConstants.SSC_CHECKPOINT_DIR_PATH
import com.sparkworkshop.sparkstreaming.utilities.StreamingUtility._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Try

object AlarmOnLogErrors {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("AlarmOnLogErrors").setMaster("local[*]")
    val ssc = StreamingContext.getOrCreate(SSC_CHECKPOINT_DIR_PATH, () => createStreamingContext(sparkConf))

    // close streaming context
    closeStreamingContext(ssc)
  }

  def createStreamingContext(sparkConf: SparkConf): StreamingContext = {
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // set log level
    setLogLevel()

    // retrieve apache log pattern
    val pattern: Pattern = retrieveApacheLogPattern()

    // nc -i 1 -kl 9999 < access_log.txt (broadcast txt file content over port 9999 with 1 sec delay b/w lines)
    val logsDStream = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    val statusCodeDStream = logsDStream.map(log => {

      val matcher = pattern.matcher(log)
      if (matcher.matches()) {
        matcher.group(6)
      } else {
        "[ERROR]"
      }
    })

    val statusDStream = statusCodeDStream.map(statusCode => {
      val code = Try(statusCode.toInt) getOrElse 0

      if (code >= 200 && code < 300) {
        "SUCCESS"
      } else if (code >= 500 && code < 600) {
        "ERROR"
      } else {
        "OTHER"
      }
    })

    // window-interval: 60 minutes and sliding-window-interval: 1 sec
    val resultDStream = statusDStream.countByValueAndWindow(Seconds(3600), Seconds(1))

    // using thread-safe counter
    val previousTime = new AtomicLong(0)

    resultDStream.foreachRDD((rdd, time) => {

      // counter
      var totalSuccessCount: Long = 0
      var totalErrorCount: Long = 0

      if (rdd.count() > 0) {

        val result = rdd.collect()
        result.foreach(status => {

          val statusString = status._1
          val statusCount = status._2

          if ("SUCCESS" == statusString) {
            totalSuccessCount += statusCount
          } else if ("ERROR" == statusString) {
            totalErrorCount += statusCount
          }
        })
      }

      // alarming in case of error ratio > 0.5 and time-interval > 10 seconds
      if (totalSuccessCount + totalErrorCount > 100) {

        print("Success Count: " + totalSuccessCount)
        print(", Error Count: " + totalErrorCount)

        println()

        // avoid divide-by-zero exception
        val errorRatio = Try(totalErrorCount.toDouble / totalSuccessCount.toDouble) getOrElse 1.0

        if (errorRatio > 0.5) {

          val currentTime = time.milliseconds
          val seconds = (currentTime - previousTime.get()) / 1000

          if (seconds > 10) {
            println("Wake somebody up! Something is horribly wrong.")
            previousTime.set(currentTime)
          }
        }
      }
    })

    ssc.checkpoint(SSC_CHECKPOINT_DIR_PATH)
    ssc
  }
}