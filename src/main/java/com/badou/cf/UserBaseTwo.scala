package com.badou.cf

import breeze.numerics.{pow, sqrt}
import org.apache.spark.sql.SparkSession

object UserBaseTwo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("just kismet")
      .enableHiveSupport()
      .getOrCreate()

    //通过相似用户来推荐商品集合
    val udata = spark.sql("select * from udata")
    val udata_v = udata.selectExpr("user_id as user_v", "item_id", "rating as rating_v")
    //1,计算用户相似度,并按照相似度倒排
    //1.1 计算规则a*b/(|a|*|b|)
    // 规则化分母|a|
    import spark.implicits._
    val user_sqrt = udata.rdd.map(x => (x(0).toString, x(2).toString))
      .groupByKey()
      .mapValues(x => sqrt(x.toList.map(rating => pow(rating.toDouble, 2)).sum))
      .toDF("user_id", "sqrt_rating")
    val user_sqrt_v = user_sqrt.selectExpr("user_id as user_v", "sqrt_rating as sqrt_rating_v")
    // 计算点乘结果a*b
    import org.apache.spark.sql.functions._
    val dot_udf = udf((rateing: Int, rateing_v: Int) => rateing * rateing_v)
    val dot = udata.join(udata_v, "item_id")
      .filter("user_id != user_v")
      .withColumn("dot", dot_udf(col("rating"), col("rating_v")))
      .selectExpr("user_id", "user_v", "dot")
      .groupBy("user_id", "user_v")
      .agg(sum("dot").as("sim"))
      .selectExpr("user_id", "user_v", "sim")
    // 计算规则a*b/(|a|*|b|)
    val sim = dot.join(user_sqrt, "user_id").join(user_sqrt_v, "user_v")
      .selectExpr("user_id", "user_v", "sim / (sqrt_rating * sqrt_rating_v) as cosine_sim ")
    // 按照user_id分组
    //2,过滤已经当前用户已经打分的物品,并给物品推荐打分(用户相似度*打分)
    //2.1 构建user--item-rating集合
    val userItemList = udata.rdd.map(x => (x(0).toString, (x(1).toString, x(2).toString)))
      .groupByKey().mapValues(_.toList.map(x => (x._1 + "-" + x._2)))
      .toDF("user_id", "item_rate_list")
    val userItemList_v = userItemList.selectExpr("user_id as user_v", "item_rate_list as item_rate_list_v")

    val u_u_itemList = sim.join(userItemList, "user_id").join(userItemList_v, "user_v")
    //过滤udf
    val filter_udf = udf { (items: Seq[String], items_v: Seq[String], cosine_sim: Double) =>
      //获取已评论的集合
      val itemList = items.map { x =>
        val l = x.split("-")
        (l(0), l(1))
      }.toMap
      //对相识商品集合进行操作
      items_v.filter { x =>
        val l = x.split("-")
        itemList.getOrElse(l(0), -1) == -1
      }.map { x =>
        val l = x.split("-")
        (l(0), l(1).toDouble * cosine_sim)
      }
    }

    val itemsimRating = u_u_itemList.withColumn("itemsimRating",
      filter_udf(col("item_rate_list"), col("item_rate_list_v"), col("cosine_sim")))
      .selectExpr("user_id", "itemsimRating")

    val userItemScore = itemsimRating.select(itemsimRating("user_id"), explode(itemsimRating("itemsimRating")))
      .toDF("user_id", "itemsimRating")
      .selectExpr("user_id", "itemsimRating._1 as item_id", "itemsimRating._2 as score")
      .groupBy("user_id", "item_id")
      .agg(sum("score").as("sum_score"))
      .orderBy(col("sum_score").desc)
  }
}
