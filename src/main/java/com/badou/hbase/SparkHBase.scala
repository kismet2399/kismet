package com.badou.hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Put}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object SparkHBase {
  //从hive取数据(Dataframe->RDD)写入HBase,
  def main(args: Array[String]): Unit = {
    //HBase zookeeper
    val zkHostIP = Array("10", "11", "12").map("192.168.181." + _)
    val ZK_QUORUM = zkHostIP.mkString(",")
    val spark = SparkSession.builder().appName("spark to HBase")
      .enableHiveSupport().getOrCreate()

    val rdd = spark.sql("select order_id,user_id,order_dow from orders limit 300").rdd

    val logger = Logger.getLogger("org.apache.spark")
    logger.error("Logger=========================================================================")
    logger.error(rdd.count())
    var list: List[String] = List()
    val result = rdd.map(row =>row(0).asInstanceOf[String] + "=========" + row(1).asInstanceOf[String] + "=========" + row(2).asInstanceOf[String] + "=========").collect().toList
    logger.error(result)
    logger.error("Logger=========================================================================")
    System.out.println("print===================================================================")
    System.out.println(rdd.count())
    System.out.println(result)

    /**
      * 一个put对象就是一行记录,在构造方法中指定主键user_id
      * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
      */
    rdd.map { row =>
      val order_id = row(0).asInstanceOf[String]
      val user_id = row(1).asInstanceOf[String]
      val order_dow = row(2).asInstanceOf[String]
      var p = new Put(Bytes.toBytes(user_id))
      p.add(Bytes.toBytes("id"), Bytes.toBytes("order"), Bytes.toBytes(order_id))
      p.add(Bytes.toBytes("num"), Bytes.toBytes("dow"), Bytes.toBytes(order_dow))
      p
      p
    }.foreachPartition { partition =>
      //初始化jobConf,TableOutputFormat必须在org.apache.hadoop.hbase.mapred包下
      val jobConf = new JobConf(HBaseConfiguration.create())
      jobConf.set("hbase.zookeeper.quorum", ZK_QUORUM)
      jobConf.set("hbase.zookeeper.property.clientPort", "2181")
      jobConf.set("zookeeper.znode.parent", "/hbase")
      jobConf.setOutputFormat(classOf[TableOutputFormat])//使用mr方式写写文件
      //写入表名
      //val table = new HTable(jobConf, TableName.valueOf("orders"))
      val connection = ConnectionFactory.createConnection(jobConf)
      val table = connection.getTable(TableName.valueOf("orders"))
      import scala.collection.JavaConversions._
      table.put(seqAsJavaList(partition.toSeq))
    }
  }
}
