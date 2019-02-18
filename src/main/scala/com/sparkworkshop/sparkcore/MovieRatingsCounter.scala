package com.sparkworkshop.sparkcore

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

// singleton object/instance in scala
object MovieRatingsCounter {

  // main entry point
  def main(args: Array[String]): Unit = {

    // log level - error
    Logger.getLogger("org").setLevel(Level.ERROR)

    // file path
    val path = System.getProperty("user.dir") + "/ratings.csv"

    // spark context - encapsulates underlying process
    val sparkContext = new SparkContext("local[*]", "MovieRatingsCounter")

    // ratings.csv : structure - (userId,movieId,rating,timestamp)

    // load ratings.csv
    val dataRDD = sparkContext.textFile(path)

    // remove header
    val filterRDD = dataRDD.filter(line => !line.contains("rating"))

    // get only ratings
    val ratingsRDD = filterRDD.map(line => line.split(",")(2).toDouble)

    // Map[String, Long] - count by value {action} | lazy execution
    val resultMap = ratingsRDD.countByValue()

    // sort the result
    val sortedSequence = resultMap.toSeq.sortBy(seq => seq._1)(Ordering[Double].reverse)

    sortedSequence.foreach(println)
  }
}
