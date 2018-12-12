package com.sparkworkshop.sparkcore

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object WordsCounter {

  def main(args: Array[String]): Unit = {

    // log level
    Logger.getLogger("org").setLevel(Level.INFO)

    // file path
    val path = System.getProperty("user.dir") + "/book.txt"

    // sortedRDD by count
    val sortedRDD = new SparkContext("local[*]", "WordsCounter")

      // this is a book
      // a book
      .textFile(path)

      // (this , is , a , book, a, book)
      .flatMap(line => line.split("\\W+"))

      // normalizing words in lowercase
      .map(word => word.toLowerCase)

      // ((this,1) , (is,1) , (a,1) , (book,1), (a,1), (book,1))
      .map(word => (word, 1))

      // ((this,1) , (is,1) , (a,2) , (book,2))
      .reduceByKey((x, y) => x + y)

      // ((1,this) , (1,is) , (2,a) , (2,book))
      .map(x => (x._2, x._1))

      // sort by key - transformation
      .sortByKey(ascending = false)

    sortedRDD.collect().foreach(println)
  }
}
