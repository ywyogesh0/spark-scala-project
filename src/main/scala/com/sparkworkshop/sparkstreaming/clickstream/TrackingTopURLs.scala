package com.sparkworkshop.sparkstreaming.clickstream

import java.util.regex.Pattern

import com.sparkworkshop.sparkstreaming.constants.StreamingConstants.SSC_CHECKPOINT_DIR_PATH
import com.sparkworkshop.sparkstreaming.utilities.StreamingUtility._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TrackingTopURLs {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("TrackingTopURLs").setMaster("local[*]")
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
    val requestDStream = logsDStream.map(log => {
      val matcher = pattern.matcher(log)
      if (matcher.matches()) matcher.group(5) else "[ERROR]"
    })

    val urlDStream = requestDStream.map(request => {
      val requestProps = request.split(" ")
      if (requestProps.length == 3) requestProps(1) else "[ERROR]"
    })

    val resultDStream = urlDStream.map(url => (url, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))
    val sortedDStream = resultDStream.transform(rdd => rdd.sortBy(_._2, ascending = false))

    sortedDStream.print()

    ssc.checkpoint(SSC_CHECKPOINT_DIR_PATH)
    ssc
  }
}