package com.badou.offline

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.alibaba.fastjson.JSON
import com.badou.streaming.Orders
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._

//import scala.util.parsing.json.JSON

object ReceiverFromKafka {
  case class Order(order_id:String,user_id:String)
  def main(args: Array[String]): Unit = {
//    if (args.length < 4) {
//      System.err.println("Usage: HdfsWordCount <directory>")
//      System.exit(1)
//    }

   // 0、对输出日志做控制
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

//    1、获取参数/指定6个参数group_id,topic,exectime,dt，ZK_QUORUM，numThreads
//    val Array(group_id,topic,exectime,dt) = args
    val Array(group_id,topic,exectime,dt) = Array("group_test","test","30","20181125")
    val zkHostIP = Array("10","11","12").map("192.168.181."+_)
    val ZK_QUORUM = zkHostIP.map(_+":2181").mkString(",")
//192.168.174.134:2181,192.168.174.125:2181,192.168.174.129:2181
    val numThreads = 1

//    2、创建streamContext
    val conf = new SparkConf()//.setAppName("TEST").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(exectime.toInt))
//    topic 对应线程Map{topic：numThreads}
    val topicSet = topic.split(",").toSet
    val topicMap = topicSet.map((_,numThreads.toInt)).toMap

//    3、通过Receiver接受kafka数据
// 为什么是map（_._2）: @return DStream of (Kafka message key, Kafka message value)
    val mesR = KafkaUtils.createStream(ssc,ZK_QUORUM,group_id,topicMap).map(_._2)

//    mesR.map((_,1L)).reduceByKey(_+_).print

//    4、生成一个rdd转DF的方法，供后面使用
    def rdd2DF(rdd: RDD[String]): DataFrame ={
      val spark = SparkSession
        .builder()
        .appName("Streaming Frome Kafka")
        .config("hive.exec.dynamic.partition","true")
        .config("hive.exec.dynamic.partition.mode","nonstrict")
        .enableHiveSupport().getOrCreate()
      import spark.implicits._
//      Json格式的数据{"order_id":"1","user_id":"2"}


      rdd.map{x=>
        val mess = JSON.parseObject(x,classOf[Orders])
        Order(mess.getOrder_id,mess.getUser_id)
      }.toDF()
    }
//  5、Dstream核心处理逻辑 ，对Dstream中每个rdd做“rdd转DF”，
//      然后通过DF结构将数据追加到分区表中
    val log = mesR.foreachRDD{rdd=>
      val df = rdd2DF(rdd)
      df.withColumn("dt",lit(dt.toString))
        .write.mode(SaveMode.Append)
        .insertInto("order_partition")
  }

    ssc.start()
    ssc.awaitTermination()
  }

}
