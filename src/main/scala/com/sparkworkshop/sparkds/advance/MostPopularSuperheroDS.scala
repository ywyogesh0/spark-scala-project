package com.sparkworkshop.sparkds.advance

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.{Codec, Source}

object MostPopularSuperheroDS {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val socialGraphPath = System.getProperty("user.dir") + "/Marvel-graph.txt"
    val socialNamesPath = System.getProperty("user.dir") + "/Marvel-names.txt"

    val sparkSession = SparkSession.builder()
      .appName("MostPopularSuperheroDS")
      .master("local[*]")
      .getOrCreate()

    import sparkSession.implicits._

    /**
      * Return map of social names
      *
      * @return
      */
    def socialNames(): Map[Int, String] = {

      var namesMap: Map[Int, String] = Map()

      // handle encoding issues
      implicit val codec: Codec = Codec("UTF-8")
      codec.onMalformedInput(CodingErrorAction.REPLACE)
      codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

      val lines = Source.fromFile(socialNamesPath).getLines()
      lines.foreach(
        line => {
          val lineArray = line.split("\"")
          if (lineArray.length > 1) {
            namesMap += lineArray(0).trim.toInt -> lineArray(1)
          }
        })

      namesMap
    }

    /**
      * Return tuple of superhero id and number of connections with other co-superhero's
      *
      * @param line superhero connections input
      * @return
      */
    def createSuperheroConnectionInput(line: String): (Int, Int) = {
      val lineArray = line.split("\\s+")
      (lineArray(0).toInt, line.length - 1)
    }

    /**
      * Get row of Dataframe
      *
      * @param df        dataframe to be mapped
      * @param bVariable broadcast variable with value as map
      * @return
      */
    def getRow(df: DataFrame, bVariable: Broadcast[Map[Int, String]]) = {
      df.map(row => (bVariable.value(row.getAs[Int]("superheroId")), row.getAs[Long]("connectionCount")))
        .toDF("superheroName", "connectionCount")
        .head()
    }

    val socialNamesVariable = sparkSession.sparkContext.broadcast(socialNames())
    val socialGraphRDD = sparkSession.sparkContext.textFile(socialGraphPath)

    // (connectionCount, superheroId)
    val connectionRDD = socialGraphRDD.map(createSuperheroConnectionInput)

    val connectionDF = connectionRDD.toDF("superheroId", "connectionCount").cache()

    connectionDF.printSchema()

    import org.apache.spark.sql.functions._

    val maxRowDF = connectionDF
      .groupBy("superheroId")
      .agg(sum("connectionCount").as("connectionCount"))
      .select("superheroId", "connectionCount")
      .orderBy(desc("connectionCount"))

    val minRowDF = connectionDF
      .groupBy("superheroId")
      .agg(sum("connectionCount").as("connectionCount"))
      .select("superheroId", "connectionCount")
      .orderBy(asc("connectionCount"))

    val maxRow = getRow(maxRowDF, socialNamesVariable)
    val minRow = getRow(minRowDF, socialNamesVariable)

    println(s"Superhero is ${maxRow.getAs("superheroName")} " +
      s"with max connection ${maxRow.getAs("connectionCount")}")

    println(s"Superhero is ${minRow.getAs("superheroName")} " +
      s"with min connection ${minRow.getAs("connectionCount")}")

    sparkSession.close()
  }
}
