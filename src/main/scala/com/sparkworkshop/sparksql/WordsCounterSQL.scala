package com.sparkworkshop.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WordsCounterSQL {

  def main(args: Array[String]): Unit = {

    // log level
    Logger.getLogger("org").setLevel(Level.INFO)

    // user dir
    val path = System.getProperty("user.dir") + "/book.txt"

    // spark session - used for sql (encapsulates spark and sql context)
    val sparkSession = SparkSession.builder().appName("WordsCounterSQL").master("local[*]").getOrCreate()

    // this is a book
    // a book
    val dataRDD = sparkSession.sparkContext.textFile(path)

      // (this , is , a , book, a, book)
      .flatMap(line => line.split("\\W+"))

      // normalizing words in lowercase
      .map(word => Words(word.toLowerCase))

    // infer the schema
    import sparkSession.implicits._
    val dataDF = dataRDD.toDF()

    dataDF.printSchema()

    // register the dataset as a table/view
    dataDF.createOrReplaceTempView("words")

    // execute sql query
    val resultDF = sparkSession
      .sql("select count(word) as count,word from words group by word order by count desc")

    // display the result (top 20 rows by default)
    resultDF.show()

    //resultDF.collect().foreach(println)

    sparkSession.close()

  }

  case class Words(word: String)

}
