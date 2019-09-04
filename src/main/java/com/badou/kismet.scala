package com.badou

import org.apache.spark.sql.SparkSession
import java.io.FileInputStream
import java.util.Properties

import org.apache.log4j.{Level, Logger}
object kismet {

  val conf = loadConf
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("kismet")
      //      .master("local[2]")
      .getOrCreate()
    val log = Logger.getLogger("org.apache.spark")
    log.error("========================================================")
    log.error("========================================================")
    log.info(conf)
    log.error("========================================================")
    log.error("========================================================")
    log.error("========================================================")
    val df = spark.sql("select * from orders")
    import spark.implicits._
    val orders = spark.sql("select * from orders")
    val priors = spark.sql("select * from order_products_prior")
    val op = orders.join(priors, "order_id").select("user_id", "product_id")

    val orderNumberSort = df.select("user_id", "order_number", "order_hour_of_day")
      .rdd.map(x => (x(0).toString, (x(1).toString, x(2).toString)))
      .groupByKey()
      .mapValues(_.toList.sortWith(_._2 < _._2))
      .toDF("user_id", "ons")
    println(orderNumberSort)
  }

  def loadConf = {
    val props = new Properties()
    props.load(new FileInputStream("case.properties"))
    props
  }
}
