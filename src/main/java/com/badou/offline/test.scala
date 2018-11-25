package com.badou.offline

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WC").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(2))
    ssc.checkpoint("E:/streamData/checkpoint")

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val addFunc = (curValues:Seq[Long],preValueState:Option[Long])=>{
      val curCount = curValues.sum
      val preCount = preValueState.getOrElse(0L)
      Some(curCount+preCount)
    }

    val lines = ssc.socketTextStream("192.168.181.10",9999)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map((_,1L)).updateStateByKey[Long](addFunc)
//      .reduceByKeyAndWindow((a:Int,b:Int)=>a+b,Seconds(20),Seconds(4))
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
