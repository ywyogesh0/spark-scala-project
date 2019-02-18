package com.sparkworkshop.sparkds.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WordsCounterDS {

  def main(args: Array[String]): Unit = {

    // log level
    Logger.getLogger("org").setLevel(Level.ERROR)

    // user dir
    val path = System.getProperty("user.dir") + "/book.txt"

    // spark session - used for sql (encapsulates spark and sql context)
    val sparkSession = SparkSession.builder().appName("WordsCounterDS").master("local[*]").getOrCreate()

    // this is a book
    // a book
    val dataRDD = sparkSession.sparkContext.textFile(path)

      // (this , is , a , book, a, book)
      .flatMap(line => line.split("\\W+"))

      // normalizing words in lowercase
      .map(word => Words(word.toLowerCase))

    // infer the schema
    import sparkSession.implicits._
    val dataDS = dataRDD.toDS()

    dataDS.printSchema()

    import org.apache.spark.sql.functions._
    val resultDF = dataDS
      .groupBy("word")
      .agg(count("word").as("count"))
      .select("count", "word")
      .sort(desc("count"))

    // display the result (top 20 rows by default)
    resultDF.show()

    //resultDF.collect().foreach(println)

    sparkSession.close()

  }

  case class Words(word: String)

}
