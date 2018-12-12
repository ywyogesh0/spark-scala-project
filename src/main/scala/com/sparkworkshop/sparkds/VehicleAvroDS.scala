package com.sparkworkshop.sparkds

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object VehicleAvroDS {

  def main(args: Array[String]): Unit = {

    // log level - error
    Logger.getLogger("org").setLevel(Level.INFO)

    // file path
    val path = System.getProperty("user.dir") + "/vehicle"
    val avroSchemaPath = System.getProperty("user.dir") + "/vehicle.avsc"

    // spark session - used for sql (encapsulates spark and sql context)
    val sparkSession = SparkSession.builder().appName("VehicleAvroDS").master("local[*]").getOrCreate()

    // write avro
    import sparkSession.implicits._
    val vehicleDS = sparkSession.createDataFrame(
      Seq(
        ("Mahindra XUV 1", "Black", 2010),
        ("Mahindra XUV 2", "Black", 2014),
        ("Mahindra XUV 3", "Black", 2015),
        ("Mahindra XUV 4", "Black", 2018),
        ("Mahindra XUV 5", "Black", 2017),
        ("Mahindra XUV 6", "Black", 2019)
      )
    ).toDF("name", "color", "model")
      .as[Vehicle]

    vehicleDS.printSchema()

    vehicleDS.write
      .format("com.databricks.spark.avro")
      .partitionBy("model")
      .save(path)

    // read avro
    import org.apache.avro.Schema
    val schema = new Schema.Parser().parse(new File(avroSchemaPath))

    val resultDS = sparkSession.read
      .format("com.databricks.spark.avro")
      .option("avroSchema", schema.toString)
      .load(path)
      .as[Vehicle]

    resultDS.show()

    sparkSession.close()
  }

  case class Vehicle(name: String, color: String, model: Int)

}
