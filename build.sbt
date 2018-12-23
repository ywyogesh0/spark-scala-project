name := "spark-scala-project"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.2",
  "org.apache.spark" %% "spark-streaming" % "2.3.2",
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.3.2",
  "org.twitter4j" % "twitter4j-core" % "4.0.6",
  "org.twitter4j" % "twitter4j-stream" % "4.0.6",
  "org.apache.spark" %% "spark-sql" % "2.3.2",
  "com.databricks" %% "spark-avro" % "4.0.0"
)
