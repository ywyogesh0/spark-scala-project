package com.sparkworkshop.sparkstreaming.basic

import com.sparkworkshop.sparkstreaming.constants.StreamingConstants._
import com.sparkworkshop.sparkstreaming.utilities.StreamingUtility._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SaveTwitterTweets {

  def main(args: Array[String]): Unit = {

    // set twitter credentials
    setTwitterCredentials()

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SaveTwitterTweets")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // set log level
    setLogLevel()

    val twitterStatusDStream = TwitterUtils.createStream(ssc, None)
    val twitterTextDStream = twitterStatusDStream.map(status => status.getText)

    var twitterTextCount: Long = 0

    twitterTextDStream.foreachRDD((rdd, time) => {

      val count = rdd.count()
      if (count > 0) {
        val repartitionRDD = rdd.repartition(1).cache()

        repartitionRDD.saveAsTextFile(TWITTER_TEXT_STREAM_DIR_PATH + time.milliseconds.toString)
        twitterTextCount += count

        println("Twitter Text Count : " + twitterTextCount)

        if (twitterTextCount > TWITTER_MAX_TEXT_COUNT) {
          ssc.stop(stopSparkContext = true, stopGracefully = false)
        }
      }
    })

    // close streaming context
    closeStreamingContext(ssc)
  }
}
