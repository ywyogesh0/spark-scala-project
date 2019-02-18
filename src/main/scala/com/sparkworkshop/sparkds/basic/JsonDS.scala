package com.sparkworkshop.sparkds.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

// singleton object/instance in scala
object JsonDS {

  // main entry point
  def main(args: Array[String]): Unit = {

    // log level - error
    Logger.getLogger("org").setLevel(Level.ERROR)

    // file path
    val path = System.getProperty("user.dir") + "/vehicle.json"

    // spark session - used for sql (encapsulates spark and sql context)
    val sparkSession = SparkSession.builder().appName("JsonDS").master("local[*]").getOrCreate()

    // load vehicle.json
    val dataDF =
      sparkSession.read.format("json").option("multiline", "true")
        .load(path)

    import sparkSession.implicits._
    val dataDS = dataDF.as[Vehicle]

    dataDS.printSchema()

    dataDS
      .filter(vehicle => vehicle.color.equalsIgnoreCase("brown"))
      .show()

    sparkSession.close()

  }

  case class Vehicle(name: String, color: String, model: Long)

}
