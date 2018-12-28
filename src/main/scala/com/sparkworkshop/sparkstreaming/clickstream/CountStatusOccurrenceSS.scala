package com.sparkworkshop.sparkstreaming.clickstream

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Locale
import java.util.regex.Pattern

import com.sparkworkshop.sparkstreaming.constants.StreamingConstants._
import com.sparkworkshop.sparkstreaming.utilities.StreamingUtility._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{Row, SparkSession}

import scala.util.Try

object CountStatusOccurrenceSS {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("CountStatusOccurrenceSS").master("local[*]")
      .getOrCreate()

    // setting log level
    setLogLevel()

    // retrieve apache access log pattern
    val pattern = retrieveApacheLogPattern()

    val logsDF = sparkSession.readStream.text(LOGS_DIR_PATH)

    import sparkSession.implicits._
    val statusDF = logsDF.flatMap(log => parseLog(log, pattern)).select("status", "dateTime")

    import org.apache.spark.sql.functions._
    val resultDF = statusDF
      .groupBy($"status", window($"dateTime", "1 hour")).count()
      .orderBy($"window")

    val streamingQuery = resultDF.writeStream.option("truncate", "false").format("console")
      .outputMode(OutputMode.Complete().toString).start()

    streamingQuery.awaitTermination()

    // stop spark session
    stopSparkSession(sparkSession)
  }

  def parseLog(log: Row, pattern: Pattern): Option[LogEntry] = {
    val matcher = pattern.matcher(Try(log.getString(0)) getOrElse "")
    if (matcher.matches()) {
      Some(LogEntry(
        matcher.group(1),
        matcher.group(2),
        matcher.group(3),
        parseDateTimeString(matcher.group(4)),
        matcher.group(5),
        matcher.group(6),
        matcher.group(7),
        matcher.group(8),
        matcher.group(9)
      ))
    } else {
      None
    }
  }

  def parseDateTimeString(dateTime: String): String = {
    val matcher = Pattern.compile("\\[(.*?) .+?]").matcher(dateTime)
    if (matcher.find()) {
      val dateTimeFormatter = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
      val date = dateTimeFormatter.parse(matcher.group(1))

      new Timestamp(date.getTime).toString
    } else ""
  }

  /** case class to infer schema for Log Entries. */
  case class LogEntry(ip: String, client: String, user: String, dateTime: String, request: String, status: String,
                      bytes: String, referer: String, agent: String)

}
