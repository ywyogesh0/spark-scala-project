package com.sparkworkshop.sparkstreaming.basic

import java.util.Properties

import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.slf4j.LoggerFactory

//This class is for consuming data from kafka topic and filtering the data and the passing the filtered data to next topic
object AccountSortCodeConsumer extends App {

  val logger = LoggerFactory.getLogger(AccountSortCodeConsumer.getClass.getName)

  val conf = new SparkConf().setMaster("local[2]").setAppName("KeyMatch")

  val ssc = new StreamingContext(conf, Seconds(5))

  //Will broadcast(send a copy of) the Account number which we want to search , to each executor for faster lookup
  val accList = List("12345", "67891")
  val accountList = ssc.sparkContext.broadcast(accList)


  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "127.0.0.1:9092",
    "key.deserializer" -> classOf[StringDeserializer].getName,
    "value.deserializer" -> classOf[StringDeserializer].getName,
    "group.id" -> "my-first",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean) // will commit offset only after processing of records to next topic
  )

  //Dstream will create as many partition as are there in topic to run them parallely
  val topics = Array("first_topic")
  val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )


  stream.foreachRDD {
    rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      // in each partition we will perform filtration of records for faster processing and parallelism
      // and for each partition we will also create a producer for parallelism and faster processing
      rdd.foreachPartition(
        partitionOfRecords => {
          // filtering only specified account number which is the key in the message
          val filteredRecord = partitionOfRecords.filter(y => accountList.value.contains(y.key().substring(1, y.key().length - 1)))

          val properties: Properties = new Properties
          properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
          properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
          properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
          properties.setProperty(ProducerConfig.RETRIES_CONFIG, "10")

          val producer = new KafkaProducer[String, String](properties)

          filteredRecord.foreach({
            y =>
              val record = new ProducerRecord[String, String]("second-topic", y.toString)
              producer.send(record, new Callback() {
                override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
                  logger.info("topic:= " + recordMetadata.topic() + " offset:= " + recordMetadata.offset())
                  logger.info(" partition:= " + recordMetadata.partition())
                }
              })
          })
        }
      )
      //only after pushing to the topic will commit the offset for safer purpose
      // because we have disabled auto commit at the start only
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }

  }

  // Start the computation
  ssc.start()
  ssc.awaitTermination()
}
