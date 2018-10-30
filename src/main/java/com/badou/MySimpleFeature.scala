package com.badou

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object MySimpleFeature {
  def Feat(priors: DataFrame, orders: DataFrame): DataFrame = {
    /** product feature
      * 1.销售量 prod_cnt
      * 2.商品被再次购买（reordered）量prod_sum_rod
      * 3.统计reordered比率 prod_rod_rate
      * */
    //1.
    import priors.sparkSession.implicits._
    priors.select("product_id").groupBy("product_id").count()
    priors.groupBy("product_id")
      .agg(sum("reordered").as("prod_sum_rod"),
        avg("reordered").as("prod_rod_rate"),
        count("product_id").as("prod_sum_rod"))

    /**
      * user Features:
      * 1. 每个用户购买订单的平均间隔
      * 2. 每个用户的总订单数
      * 3. 每个用户购买的product商品去重后的集合数据
      * 4. 用户总商品数量以及去重后的商品数量
      * 5. 每个用户购买的平均每个订单商品数量
      **/

    //1. 每个用户购买订单的平均间隔
    val op = priors.join(orders, "order_id")
    val u_ds = orders.selectExpr("user_id", "if(days_since_prior_order=='',0,days_since_prior_order) as ds")
      .groupBy("user_id")
      .agg(avg("ds"))
      .withColumnRenamed("avg(ds)", "ds_avg")
    //2. 每个用户的总订单数
    val u_order_total = orders.groupBy("user_id").count()
    //3. 每个用户购买的product商品去重后的集合数据
    val u_prods = op.select("user_id", "product_id").distinct()
      .rdd.map(x => (x(0).toString, x(1).toString))
      .groupByKey().mapValues(_.toList.mkString(","))
      .toDF("user_id", "prod_recoder")
    //4. 用户总商品数量以及去重后的商品数量
    val u_pro_tal = op.select("user_id", "product_id")
      .rdd.map(x => (x(0).toString, x(1).toString))
      .groupByKey().mapValues(x => (x.toList.size.toString, x.toSet.mkString(",")))
      .toDF("user_id", "tem")
      .selectExpr("user_id", "tem._1 as pro_tal", "tem._2 as pro_record")
    //5. 每个用户购买的平均每个订单商品数量
    val u_pro_avg = priors
      .groupBy("order_id").count().join(orders, "order_id")
      .groupBy("user_id").agg(avg("count").as("u_pro_avg"))
    /**
      * user and product Feature: cross feature 交叉特征
      * 1. 统计user和对应product在多少个订单中出现（distinct order_id）
      * 2. 特定product具体在购物车中的出现位置的平均位置
      * 3. 最后一个订单id
      * 4. 用户对应product在所有这个用户购买产品量中的占比rate
      **/
    //1. 统计user和对应product在多少个订单中出现（distinct order_id）
    val u_p_record = priors.selectExpr("product_id","count(product_id) as prod_ord")
  }

}