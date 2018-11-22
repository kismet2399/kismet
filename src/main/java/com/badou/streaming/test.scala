package com.badou.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object test {
  def main(args: Array[String]): Unit = {
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

    val lines = ssc.socketTextStream("192.168.181.10", 9999)
    val words = lines.flatMap(_.split(" "))
    val wordCount = words.map((_, 1L))
        .updateStateByKey[Long](addFunc)
    //.reduceByKeyAndWindow((a:Int,b:Int)=>a+b,Seconds(20),Seconds(2))
    wordCount.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
