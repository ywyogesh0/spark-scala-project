package com.sparkworkshop.sparkds.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

// singleton object/instance in scala
object MovieRatingsCounterDS {

  // main entry point
  def main(args: Array[String]): Unit = {

    // log level - error
    Logger.getLogger("org").setLevel(Level.ERROR)

    // file path
    val path = System.getProperty("user.dir") + "/ratings.csv"

    // spark session - used for sql (encapsulates spark and sql context)
    val sparkSession = SparkSession.builder().appName("MovieRatingsCounterDS").master("local[*]").getOrCreate()

    // ratings.csv : structure - (userId,movieId,rating,timestamp)

    val schema = StructType(Seq(
      StructField("userId", DataTypes.IntegerType, nullable = true),
      StructField("movieId", DataTypes.IntegerType, nullable = true),
      StructField("rating", DataTypes.DoubleType, nullable = true),
      StructField("timestamp", DataTypes.LongType, nullable = true)
    ))

    val dataDF =
      sparkSession.read.format("csv").option("header", "true").schema(schema)
        .load(path)

    import sparkSession.implicits._
    val dataDS = dataDF.as[Ratings]

    dataDS.printSchema()

    import org.apache.spark.sql.functions._
    dataDS.groupBy("rating")
      .agg(count("rating").as("count"))
      .select("rating", "count")
      .sort(asc("rating"))
      .show()

    sparkSession.close()
  }

  case class Ratings(userId: Int, movieId: Int, rating: Double, timestamp: Long)

}
