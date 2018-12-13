package com.sparkworkshop.sparkds

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.io.{Codec, Source}

object MostPopularMovieDS {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val ratingsCsvPath = System.getProperty("user.dir") + "/ratings.csv"
    val moviesCsvPath = System.getProperty("user.dir") + "/movies.csv"

    def loadMovieTitles(): Map[Int, String] = {
      var movieTitles: Map[Int, String] = Map()

      // handling encoding issues
      implicit val codec: Codec = Codec("UTF-8")
      codec.onMalformedInput(CodingErrorAction.REPLACE)
      codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

      val movieLines = Source.fromFile(moviesCsvPath).getLines()
      movieLines.foreach(movieLine => {
        val movieLineArray = movieLine.split(",")
        if (!movieLineArray.contains("movieId")) {
          movieTitles += (movieLineArray(0).toInt -> movieLineArray(1))
        }
      })

      movieTitles
    }

    val sparkSession = SparkSession.builder().appName("MostPopularMovieDS").master("local[*]").getOrCreate()
    val ratingsDF = sparkSession.read.format("csv").option("header", "true").load(ratingsCsvPath)

    val movieBCast = sparkSession.sparkContext.broadcast(loadMovieTitles())

    import sparkSession.implicits._
    import org.apache.spark.sql.functions._
    val moviesDS = ratingsDF.groupBy("movieId")
      .agg(count("movieId").as("count"))
      .select("count", "movieId")
      .orderBy(desc("count"))
      .toDF("count", "movieId").as[MoviesResult]

    moviesDS.map(movie => (movie.count, movieBCast.value(movie.movieId.toInt)))
      .toDF("count", "title")
      .show(truncate = false)

    sparkSession.close()
  }

  case class MoviesResult(count: BigInt, movieId: String)

}
