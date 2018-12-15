package com.sparkworkshop.sparkds.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

// singleton object/instance in scala
object FriendsAverageByAgeDS {

  // main entry point
  def main(args: Array[String]): Unit = {

    // log level - error
    Logger.getLogger("org").setLevel(Level.INFO)

    // file path
    val path = System.getProperty("user.dir") + "/friends.csv"

    // spark session - used for sql (encapsulates spark and sql context)
    val sparkSession = SparkSession.builder().appName("FriendsAverageByAgeDS").master("local[*]").getOrCreate()

    // friends.csv : structure - (id,name,age,friendsCount)

    val schema = StructType(Seq(
      StructField("id", DataTypes.IntegerType, nullable = true),
      StructField("name", DataTypes.StringType, nullable = true),
      StructField("age", DataTypes.IntegerType, nullable = true),
      StructField("friendsCount", DataTypes.IntegerType, nullable = true)
    ))

    // load friends.csv
    val dataDF =
      sparkSession.read.format("csv").option("header", "false").schema(schema)
        .load(path)

    import sparkSession.implicits._
    val dataDS = dataDF.as[Friends]

    dataDS.printSchema()

    import org.apache.spark.sql.functions._
    dataDS.groupBy("age")
      .agg(avg("friendsCount").as("friendsCountAverage"))
      .select("age", "friendsCountAverage")
      .sort(asc("age"))
      .show()

    sparkSession.close()
  }

  case class Friends(id: Int, name: String, age: Int, friendsCount: Int)

}
