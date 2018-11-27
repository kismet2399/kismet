package com.badou.streaming

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.functions._

object ReceiverFromKafka {
  case class Order(order_id:String,user_id:String)
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println("Usage: ReceiverFromKafka<directory>")
      System.exit(1)
    }
    val Array(group_id, topic, exectime, dt) = args;
    val zkHostIp = Array("10", "11", "12").map("192.168.181." + _)
    val ZK_QUQRUM = zkHostIp.map(_ + ":2181").mkString(",")
    val numThreads = 1

    //创建streamContext
    val conf = new SparkConf()
    val ssc = new StreamingContext(conf, Seconds(exectime.toInt))
    val set = topic.split(",").toSet
    //topic 对应线程池数量
    val topicMap = set.map((_, numThreads.toInt)).toMap
    //通过receive的方式接受kafka消息
    val mesR = KafkaUtils.createStream(ssc,ZK_QUQRUM,group_id,topicMap).map(_._2)

    def rdd2Df(rdd: RDD[String]):DataFrame = {
      val spark = SparkSession
        .builder()
        .appName("Streaming from Kafka")
        .config("hive.exec.dynamic.partition","true")
        .config("hive.exec.dynamic.partition.mode","nonstrict")
        .enableHiveSupport()
        .getOrCreate()
      import spark.implicits._
      rdd.map{x=>
        val mess = JSON.parseObject(x,classOf[Orders])
        Order(mess.getOrder_id,mess.getUser_id)
      }.toDF()
    }
    val log = mesR.foreachRDD{rdd=>
      val df = rdd2Df(rdd)
      df.withColumn("dt",lit(dt))
        .write.mode(SaveMode.Append)
        .insertInto("order_partition")
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
