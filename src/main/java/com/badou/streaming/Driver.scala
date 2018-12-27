package com.badou.streaming

import org.apache.spark.SparkConf

import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils

object Driver {
  
  def main(args: Array[String]): Unit = {
    
    //--启动线程数，至少是两个。一个线程用于监听数据源，其他线程用于消费或打印。至少是2个
    val conf=new SparkConf().setMaster("local[5]").setAppName("kafkainput")
    
    val sc=new SparkContext(conf)
    
    val ssc=new StreamingContext(sc,Seconds(5))
    //ssc.checkpoint("d://check1801")
    
    //--连接kafka,并消费数据
    val zkHosts="192.168.181.10:2181,192.168.181.11:2181,192.168.181.12:2181"
    val groupName="group-top333"
    //--Map的key是消费的主题名，value是消费的线程数。也可以消费多个主题，比如：Map("parkx"->1,"enbook"->2)
    val topic=Map("top333"->1)
    
    //--获取kafka的数据源
    //--SparkStreaming作为Kafka消费的数据源，即从kafka中消费的偏移量(offset)存到zookeeper上
    val kafkaStream=KafkaUtils.createStream(ssc, zkHosts, groupName, topic).map{data=>data._2}
    
//    val wordcount=kafkaStream.flatMap { line =>line.split(" ") }.map { word=>(word,1) }
//                  .updateStateByKey{(seq,op:Option[Int])=>Some(seq.sum+op.getOrElse(0))}
//    
//    wordcount.print()
    kafkaStream.print()
    
    ssc.start()
    
    //--保持SparkStreaming线程一直开启
    ssc.awaitTermination()
  }
}