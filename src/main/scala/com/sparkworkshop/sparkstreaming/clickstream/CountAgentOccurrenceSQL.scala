package com.sparkworkshop.sparkstreaming.clickstream

import java.util.regex.Pattern

import com.sparkworkshop.sparkstreaming.constants.StreamingConstants._
import com.sparkworkshop.sparkstreaming.utilities.StreamingUtility._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Try

object CountAgentOccurrenceSQL {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("CountAgentOccurrenceSQL").setMaster("local[*]")
    val ssc = createStreamingContext(sparkConf)

    // set streaming conf
    setStreamingConfiguration(ssc)
  }

  def createStreamingContext(sparkConf: SparkConf): StreamingContext = {
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // set log level
    setLogLevel()

    // retrieve apache log pattern
    val pattern: Pattern = retrieveApacheLogPattern()

    // nc -i 1 -kl 9999 < access_log.txt (broadcast txt file content over port 9999 with 1 sec delay b/w lines)
    val logsDStream = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    // (url, status, agent)
    val recordDStream = logsDStream.map(log => {

      val matcher = pattern.matcher(log)
      if (matcher.matches()) {

        val requestArray = matcher.group(5).split(" ")
        val url = if (requestArray.length == 3) requestArray(1) else "[ERROR]"

        val status = Try(matcher.group(6).toInt) getOrElse 0
        val agent = matcher.group(9)

        (url, status, agent)

      } else ("[ERROR]", 0, "[ERROR]")

    })

    val sparkSession = SparkSessionSingleton.getInstance(sparkConf)

    import sparkSession.implicits._
    recordDStream.foreachRDD(foreachFunc = (recordRDD, time) => {

      val recordDF = recordRDD
        .map(record => Record(record._1, record._2, record._3))
        .filter(record => !record.agent.trim.isEmpty && !record.agent.trim.contains("-")).toDF()

      recordDF.createOrReplaceTempView("records")

      val resultDF = sparkSession
        .sql("select agent, count(1) as totalAgentCount from records group by agent").cache()

      println(s"------------$time-------------")

      if (resultDF.count() > 0) {
        resultDF.show(truncate = false)
        resultDF
          .coalesce(1)
          .write.format("json").save(CLICK_STREAM_LOGS_DIR_PATH + time.milliseconds.toString)
      }

    })

    ssc
  }

  /** Record case class to infer schema for structured record */
  case class Record(url: String, status: Int, agent: String)

  /** Lazy Spark Session Instantiation */
  object SparkSessionSingleton {

    // no need to serialize instance object
    @transient private var instance: SparkSession = _

    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession.builder().config(sparkConf).getOrCreate()
      }

      instance
    }
  }

}