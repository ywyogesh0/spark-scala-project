package com.sparkworkshop.sparkstreaming.constants

object StreamingConstants {

  // twitter
  val TWITTER4J_OAUTH_KEY = "twitter4j.oauth."
  val TWITTER_CREDENTIALS_FILE_NAME = "twitter.credentials"
  val TWITTER_TEXT_STREAM_DIR = "/twitter-text-stream/twitter-text-stream_"
  val TWITTER_TEXT_STREAM_DIR_PATH: String = System.getProperty("user.dir") + TWITTER_TEXT_STREAM_DIR
  val TWITTER_MAX_TEXT_COUNT: Long = 100

  // fault-tolerance
  val SSC_CHECKPOINT_DIR = "ssc-checkpoint"
  val SSC_CHECKPOINT_DIR_PATH: String = System.getProperty("user.dir") + "/" + SSC_CHECKPOINT_DIR

  // apache access logs
  val CLICK_STREAM_LOGS_DIR = "/click-stream/click-stream_"
  val CLICK_STREAM_LOGS_DIR_PATH: String = System.getProperty("user.dir") + CLICK_STREAM_LOGS_DIR
  val LOGS_DIR = "/logs"
  val LOGS_DIR_PATH: String = System.getProperty("user.dir") + LOGS_DIR
}
