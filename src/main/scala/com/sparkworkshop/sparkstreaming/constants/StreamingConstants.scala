package com.sparkworkshop.sparkstreaming.constants

object StreamingConstants {

  val TWITTER4J_OAUTH_KEY = "twitter4j.oauth."
  val TWITTER_CREDENTIALS_FILE_NAME = "twitter.credentials"
  val SSC_CHECKPOINT_DIR = "ssc-checkpoint"
  val SSC_CHECKPOINT_DIR_PATH: String = System.getProperty("user.dir") + "/" + SSC_CHECKPOINT_DIR
  val TWITTER_TEXT_STREAM_DIR = "/twitter-text-stream/twitter-text-stream_"
  val TWITTER_TEXT_STREAM_DIR_PATH: String = System.getProperty("user.dir") + TWITTER_TEXT_STREAM_DIR
  val CLICK_STREAM_LOGS_DIR = "/click-stream-logs/click-stream-logs_"
  val CLICK_STREAM_LOGS_DIR_PATH: String = System.getProperty("user.dir") + CLICK_STREAM_LOGS_DIR
  val TWITTER_MAX_TEXT_COUNT: Long = 100

}
