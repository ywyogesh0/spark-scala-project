package com.sparkworkshop.sparkcore

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

// singleton object/instance in scala
object FriendsAverageByAge {

  // main entry point
  def main(args: Array[String]): Unit = {

    // log level - error
    Logger.getLogger("org").setLevel(Level.ERROR)

    // file path
    val path = System.getProperty("user.dir") + "/friends.csv"

    // spark context - encapsulates underlying process
    val sparkContext = new SparkContext("local[*]", "FriendsAverageByAge")

    // friends.csv : structure - (id,name,age,friendsCount)

    // load friends.csv
    val dataRDD = sparkContext.textFile(path)

    // ex:- age : 34 , friendsCount : 45,23

    // (34, 45.0) and (34, 23.0)
    val ageByFriendsCountRDD = dataRDD.map(mapAgeByFriendsCount)

    // (34, (45.0, 1)) and (34, (23.0, 1))
    val ageByValueRDD = ageByFriendsCountRDD.mapValues(value => (value, 1))

    // (34, (68.0, 2))
    val reduceRDD = ageByValueRDD.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    // (34, 34.0)
    val resultRDD = reduceRDD.mapValues(value => value._1 / value._2)

    // Array[(Int,Double)]
    val result = resultRDD.collect()

    result.sortBy(_._1).foreach(println)

  }

  def mapAgeByFriendsCount(line: String): (Int, Double) = {
    val input = line.split(",")
    (input(2).toInt, input(3).toDouble)
  }
}
