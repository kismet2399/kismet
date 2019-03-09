package com.badou.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object test {
  def main(args: Array[String]): Unit = {
    val properties = System.getProperties()
    System.setProperty("hadoop.home.dir", "C:\\bigData\\hadoop-2.6.1")
    val conf = new SparkConf().setAppName("WC").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.checkpoint("C:/streamData/checkpoint")

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val addFunc = (current: Seq[Long], pre: Option[Long]) => {
      val curVal = current.sum
      val preval = pre.getOrElse(0L)
      Some(curVal + preval)
    }

    val lines = ssc.socketTextStream("192.168.181.10", 9998)
    val words = lines.flatMap(_.split(" "))
    val wordCount = words.map((_, 1L))
      .updateStateByKey[Long](addFunc)
    //.reduceByKeyAndWindow((a:Int,b:Int)=>a+b,Seconds(20),Seconds(2))
    wordCount.print()
    words.checkpoint(Duration(2))
    ssc.start()
    ssc.awaitTermination()
//    val conf = new SparkConf().setMaster("local[2]").setAppName("weblog")
//    val sc = new SparkContext(conf)
//    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
//    val ssc = new StreamingContext(sc, Seconds(5))
//    //ssc.checkpoint("C:/streamData/checkpoint")
//    val lines = ssc.textFileStream("hdfs://192.168.181.10:9000/input/")
//    val unit = lines.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
//    unit.print()
//    ssc.start()
//    ssc.awaitTermination()
  }

}
