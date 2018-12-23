package com.sparkworkshop.sparkstreaming.utilities

import com.sparkworkshop.sparkstreaming.constants.StreamingConstants._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.StreamingContext

import scala.io.Source

object StreamingUtility {

  def setLogLevel(): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
  }

  def setTwitterCredentials(): Unit = {
    val twitterCredentialsPath = System.getProperty("user.dir") + "/" + TWITTER_CREDENTIALS_FILE_NAME

    val lines = Source.fromFile(twitterCredentialsPath).getLines()
    for (line <- lines) {
      val credential = line.split(" ")
      System.setProperty(TWITTER4J_OAUTH_KEY + credential(0), credential(1))
    }
  }

  def setStreamingConfiguration(_ssc: StreamingContext): Unit = {
    _ssc.start()
    _ssc.awaitTermination()
  }
}
