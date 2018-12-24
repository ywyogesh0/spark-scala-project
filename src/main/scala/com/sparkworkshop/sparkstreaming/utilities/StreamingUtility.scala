package com.sparkworkshop.sparkstreaming.utilities

import java.util.regex.Pattern

import com.sparkworkshop.sparkstreaming.constants.StreamingConstants._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.StreamingContext

import scala.io.Source

object StreamingUtility {

  def setLogLevel(): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
  }

  /** Set Twitter Credentials for Authorization. */
  def setTwitterCredentials(): Unit = {
    val twitterCredentialsPath = System.getProperty("user.dir") + "/" + TWITTER_CREDENTIALS_FILE_NAME

    val lines = Source.fromFile(twitterCredentialsPath).getLines()
    for (line <- lines) {
      val credential = line.split(" ")
      System.setProperty(TWITTER4J_OAUTH_KEY + credential(0), credential(1))
    }
  }

  /** Start and Wait for Streaming Context completion. */
  def setStreamingConfiguration(_ssc: StreamingContext): Unit = {
    _ssc.start()
    _ssc.awaitTermination()
  }

  /** Retrieves a regex Pattern for parsing Apache access logs. */
  def retrieveApacheLogPattern(): Pattern = {
    val ddd = "\\d{1,3}"
    val ip = s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"
    val client = "(\\S+)"
    val user = "(\\S+)"
    val dateTime = "(\\[.+?\\])"
    val request = "\"(.*?)\""
    val status = "(\\d{3})"
    val bytes = "(\\S+)"
    val referer = "\"(.*?)\""
    val agent = "\"(.*?)\""
    val regex = s"$ip $client $user $dateTime $request $status $bytes $referer $agent"
    Pattern.compile(regex)
  }
}
