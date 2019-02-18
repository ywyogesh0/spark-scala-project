package com.sparkworkshop.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

// singleton object/instance in scala
object FriendsAverageByAgeSQL {

  // main entry point
  def main(args: Array[String]): Unit = {

    // log level - error
    Logger.getLogger("org").setLevel(Level.ERROR)

    // user dir
    val path = System.getProperty("user.dir") + "/friends.csv"

    // spark session - used for sql (encapsulates spark and sql context)
    val sparkSession = SparkSession.builder().appName("FriendsAverageByAgeSQL").master("local[*]").getOrCreate()

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

    dataDF.printSchema()

    // register the dataset as a table/view
    dataDF.createOrReplaceTempView("friends")

    // execute sql query
    sparkSession
      .sql("select age,avg(friendsCount) as avg from friends group by age order by age")
      .show()

    sparkSession.close()
  }
}
