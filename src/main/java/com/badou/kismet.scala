package com.badou

import org.apache.spark.sql.SparkSession

object kismet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("kismet")
      .master("local[2]")
      .getOrCreate();
    val df = spark.sql("select * from orders");


    import spark.implicits._
    val orders = spark.sql("select * from orders");
    val priors = spark.sql("select * from order_products_prior");
    val op = orders.join(priors,"order_id").select("user_id","product_id")

    val orderNumberSort = df.select("user_id", "order_number", "order_hour_of_day")
      .rdd.map(x => (x(0).toString, (x(1).toString, x(2).toString)))
      .groupByKey()
      .mapValues(_.toList.sortWith(_._2<_._2))
      .toDF("user_id","ons")

  }
}
