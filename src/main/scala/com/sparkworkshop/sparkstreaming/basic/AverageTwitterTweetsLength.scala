package com.sparkworkshop.sparkstreaming.basic

import java.util.concurrent.atomic.AtomicLong

import com.sparkworkshop.sparkstreaming.utilities.StreamingUtility._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object AverageTwitterTweetsLength {

  def main(args: Array[String]): Unit = {

    // set twitter credentials
    setTwitterCredentials()

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("AverageTwitterTweetsLength")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // set log level
    setLogLevel()

    val twitterStatusDStream = TwitterUtils.createStream(ssc, None)
    val twitterTextDStream = twitterStatusDStream.map(status => status.getText)
    val twitterTextLengthDStream = twitterTextDStream.map(text => text.length)

    // using thread-safe counters
    val twitterTextTotalCount = new AtomicLong(0)
    val twitterTextTotalCharactersCount = new AtomicLong(0)
    val twitterTextMaxLength = new AtomicLong(0)
    val largestTweet = new AtomicLong(0)

    twitterTextLengthDStream.foreachRDD(rdd => {

      val count = rdd.count()
      if (count > 0) {
        val repartitionRDD = rdd.repartition(1).cache()

        twitterTextTotalCount.getAndAdd(count)
        twitterTextTotalCharactersCount.getAndAdd(repartitionRDD.reduce((x, y) => x + y))
        twitterTextMaxLength.set(repartitionRDD.max())

        val averageTwitterTweetsLength = twitterTextTotalCharactersCount.get() / twitterTextTotalCount.get()

        print("Total Tweets Count: " + twitterTextTotalCount.get())
        print(", Total Tweets Characters Count: " + twitterTextTotalCharactersCount.get())
        print(", Average Tweets Length: " + averageTwitterTweetsLength)

        if (twitterTextMaxLength.get() > largestTweet.get()) {
          largestTweet.set(twitterTextMaxLength.get())
        }

        println()
        println("Till Now - Largest Tweet Length: " + largestTweet.get())
      }
    })

    // set streaming conf
    setStreamingConfiguration(ssc)
  }
}
