package com.badou.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


object DirectKafkaWC {
  def main(args: Array[String]) {
//    if (args.length < 2) {
//      System.err.println(s"""
//                            |Usage: DirectKafkaWordCount <brokers> <topics>
//                            |  <brokers> is a list of one or more Kafka brokers
//                            |  <topics> is a list of one or more kafka topics to consume from
//                            |
//        """.stripMargin)
//      System.exit(1)
//    }

//    StreamingExamples.setStreamingLogLevels()

    val Array(brokers, topics) = Array("192.168.181.10:9092", "test")

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[
      String,
      String,
      StringDecoder,
      StringDecoder](
      ssc, kafkaParams, topicsSet)

    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_._2)
//    val words = lines.flatMap(_.split(" "))
    val wordCounts = lines.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}