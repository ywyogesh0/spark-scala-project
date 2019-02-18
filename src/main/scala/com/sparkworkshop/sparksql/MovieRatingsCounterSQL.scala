package com.sparkworkshop.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

// singleton object/instance in scala
object MovieRatingsCounterSQL {

  // main entry point
  def main(args: Array[String]): Unit = {

    // log level - error
    Logger.getLogger("org").setLevel(Level.ERROR)

    // user dir
    val path = System.getProperty("user.dir") + "/ratings.csv"

    // spark session - used for sql (encapsulates spark and sql context)
    val sparkSession = SparkSession.builder().appName("MovieRatingsCounterSQL").master("local[*]").getOrCreate()

    // ratings.csv : structure - (userId,movieId,rating,timestamp)

    val schema = StructType(Seq(
      StructField("userId", DataTypes.IntegerType, nullable = true),
      StructField("movieId", DataTypes.IntegerType, nullable = true),
      StructField("rating", DataTypes.DoubleType, nullable = true),
      StructField("timestamp", DataTypes.LongType, nullable = true)
    ))

    // load ratings.csv
    val dataDF =
      sparkSession.read.format("csv").option("header", "true").schema(schema)
        .load(path)

    dataDF.printSchema()

    // register the dataset as a table/view
    dataDF.createOrReplaceTempView("ratings")

    // execute sql query
    sparkSession
      .sql("select rating,count(rating) as count from ratings group by rating order by rating")
      .show()

    sparkSession.close()

  }
}
