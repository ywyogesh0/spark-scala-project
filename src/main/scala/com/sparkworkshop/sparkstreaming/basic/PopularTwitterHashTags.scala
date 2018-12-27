package com.sparkworkshop.sparkstreaming.basic

import com.sparkworkshop.sparkstreaming.constants.StreamingConstants._
import com.sparkworkshop.sparkstreaming.utilities.StreamingUtility._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object PopularTwitterHashTags {

  def main(args: Array[String]): Unit = {

    // set twitter credentials
    setTwitterCredentials()

    // set log level
    setLogLevel()

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("PopularTwitterHashTags")
    val ssc = StreamingContext.getOrCreate(SSC_CHECKPOINT_DIR_PATH, () => createStreamingContext(sparkConf))

    // close streaming context
    closeStreamingContext(ssc)
  }

  def createStreamingContext(sparkConf: SparkConf): StreamingContext = {
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val twitterStatusDStream = TwitterUtils.createStream(ssc, None)
    val twitterTextDStream = twitterStatusDStream.map(status => status.getText)
    val twitterHashTagsDStream = twitterTextDStream.flatMap(text => text.split(" "))
      .filter(word => word.trim.startsWith("#")).map(hashtag => (hashtag, 1))

    // twitterHashTagsDStream.reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))
    val resultDSteam = twitterHashTagsDStream.reduceByKeyAndWindow((x, y) => x + y, (x, y) => x - y, Seconds(300), Seconds(1))
    val sortedDStream = resultDSteam.transform(rdd => rdd.sortBy(_._2, ascending = false))

    sortedDStream.print()

    ssc.checkpoint(SSC_CHECKPOINT_DIR_PATH)
    ssc
  }
}
