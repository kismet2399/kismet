package com.badou.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object HdfsWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: HdfsWordCount <directory>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("HdfsWordCount")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val lines = ssc.socketTextStream("192.168.181.10",9999)
    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    //val lines = ssc.textFileStream(args(0))
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    wordCounts.saveAsTextFiles(args(0))
    ssc.start()
    ssc.awaitTermination()
  }
}

