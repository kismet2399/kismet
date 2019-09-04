package com.badou.cf

import breeze.numerics.{pow, sqrt}
import org.apache.spark.sql.SparkSession

object UserBaseTwo {
  /**
    * data-->user_id,item_id,score-->方法:a*b/(|a|*|b|)
    *
    * 1,基于user_id的groupBy统计用户的打分模-->(score^2).sum在开方计算|a|与|b|
    * 2,a*b
    * 3,计算余弦相似度 cos=a*b/(|a|*|b|)
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("just kismet")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._

    //1,基于user_id的groupBy统计用户的打分模-->(score^2).sum在开方计算|a|与|b|
    val ratePow = udf((rating: Int) => pow(rating, 2))
    val sqrtRatePow = udf((rating: Int) => sqrt(rating))

    val udata = spark.sql("select user_id,item_id,rating from udata")
    val scorePow = udata.withColumn("ratePow", ratePow(col("rating"))).selectExpr("user_id", "item_id", "ratePow")
    val userSqrt = scorePow.groupBy("user_id").agg(sum("ratePow").as("sumRatePow")).withColumn("sqrt_rating", sqrtRatePow($"sumRatePow"))
      .selectExpr("user_id", "sqrt_rating")
    val userSqrt_v = userSqrt.selectExpr("user_id as user_v", "sqrt_rating as sqrt_rating_v")

    // 2,计算a*b
    val dotUdf = udf((rating: Int, rating_v: Int) => rating * rating_v)

    val udata_v = udata.selectExpr("user_id as user_v", "item_id", "rating as rating_v")
    val dot = udata.join(udata_v, "item_id")
      .filter("user_id != user_v")
      .withColumn("dot", dotUdf(col("rating"), col("rating_v")))
      .selectExpr("user_id", "user_v", "dot")
      .groupBy("user_id", "user_v")
      .agg(sum("dot").as("sim"))
      .selectExpr("user_id", "user_v", "sim")
    // 3,计算余弦相似度 cos=a*b/(|a|*|b|)
    val sim = dot.join(userSqrt, "user_id").join(userSqrt_v, "user_v")
      .selectExpr("user_id", "user_v", "sim / (sqrt_rating * sqrt_rating_v) as cosine_sim ")
    // 按照user_id分组
    //4,过滤已经当前用户已经打分的物品,并给物品推荐打分(用户相似度*打分)
    //4.1 构建user--item-rating集合
    val itemRate = udf((item: String, rating: Int) => item + "-" + rating)
    val userItemList = udata.withColumn("item_rate", itemRate($"item_id", $"rating"))
      .groupBy("user_id")
      .agg(collect_list("item_rate").as("item_rate_list"))
    val userItemList_v = userItemList.selectExpr("user_id as user_v", "item_rate_list as item_rate_list_v")

    val u_u_itemList = sim.join(userItemList, "user_id").join(userItemList_v, "user_v")
    //过滤udf
    val filter_udf = udf { (items: Seq[String], items_v: Seq[String], cosine_sim: Double) =>
      //获取已购买的集合
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

    // 过滤已经购买过的商品
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
