package com.sparkworkshop.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

// singleton object/instance in scala
object JsonSQL {

  // main entry point
  def main(args: Array[String]): Unit = {

    // log level - error
    Logger.getLogger("org").setLevel(Level.ERROR)

    // user dir
    val path = System.getProperty("user.dir") + "/vehicle.json"

    // spark session - used for sql (encapsulates spark and sql context)
    val sparkSession = SparkSession.builder().appName("JsonSQL").master("local[*]").getOrCreate()

    // load vehicle.json
    val dataDF =
      sparkSession.read.format("json").option("multiline", "true")
        .load(path)

    dataDF.printSchema()

    // register the dataset as a table/view
    dataDF.createOrReplaceTempView("vehicle")

    // execute sql query
    sparkSession
      .sql("select color,model,name from vehicle where color like 'Brown'")
      .show()

    sparkSession.close()

  }
}
