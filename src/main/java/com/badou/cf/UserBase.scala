package com.badou.cf

import breeze.numerics.{pow, sqrt}
import org.apache.spark.sql.SparkSession

object UserBase {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("User Base CF")
      .enableHiveSupport()
      .getOrCreate()
    val udata = spark.sql("select * from udata")
    // 1.计算相似度 cos=a*b/(|a|*|b|)
    //    udata.filter("user_id='198'").show()
    // 计算分母
    import spark.implicits._
    val userSoureSum = udata.rdd.map(x => (x(0).toString, x(2).toString))
      .groupByKey()
      .mapValues(x => sqrt(x.toList.map(rating => pow(rating.toDouble, 2)).sum))
      .toDF("user_id", "rating_sqrt_sum")
    // 1.1 item->user倒排表
    val df = udata.selectExpr("user_id as user_v", "item_id", "rating as rating_v")
    val df_decare = udata.join(df, "item_id")
      .filter("user_id <> user_v")
    // 对item聚合在对user聚合计算出相识度
    // 计算统一item时的评分乘积
    import org.apache.spark.sql.functions._
    val product_udf = udf((s1: Int, s2: Int) => s1.toDouble * s2.toDouble)
    val df_product = df_decare.withColumn("rating_product", product_udf(col("rating"), col("rating_v")))
      .select("user_id", "user_v", "rating_product")
    // 根据用户对来聚合
    val df_sim_group = df_product.groupBy("user_id", "user_v")
      .agg(sum("rating_product").as("rating_sum_product"))

    df_sim_group.where("user_id='198' and user_v='247'").show()

    // 计算最终结果 cos=a*b/(|a|*|b|)
    val userSoureSum_v = userSoureSum.selectExpr("user_id as user_v", "rating_sqrt_sum as rating_sqrt_sum_v")
    val df_sim = df_sim_group.join(userSoureSum, "user_id").join(userSoureSum_v, "user_v")
      .selectExpr("user_id", "user_v", "rating_sum_product / (rating_sqrt_sum * rating_sqrt_sum_v) as cosine_sim")

    //2 获取相似用户的物品集合
    //2.1 取前n个相似用户
    val df_nsim = df_sim.rdd.map(x => (x(0).toString, (x(1).toString, x(2).toString)))
      .groupByKey().mapValues(x => x.toList.sortWith(_._2 > _._2).slice(0, 10))
      .flatMapValues(x => x)
      .toDF("user_id", "user_v_sim")
      .selectExpr("user_id", "user_v_sim._1 as user_v", "user_v_sim._2 as sim")

    // 将商品通过user_id来聚合
    val df_user_item = udata.rdd.map(x => (x(0).toString, x(1).toString + "_" + x(2).toString))
      .groupByKey().mapValues(_.toList).toDF("user_id", "item_rating_arr")
    // 过滤到前面的
    val df_user_item_v = df_user_item.selectExpr("user_id as user_v", "item_rating_arr as item_rating_arr_v")

    //    |user_v|user_id|                sim|     item_rating_arr|   item_rating_arr_v|
    //    +------+-------+-------------------+--------------------+--------------------+
    //    |   467|    139|0.32266158985444504|[268_4, 303_5, 45...|[1017_2, 50_4, 15...|
    val df_gen_item = df_nsim.join(df_user_item, "user_id").join(df_user_item_v, "user_v")
    // 2.3用udf过滤相识用户user_id和user_v中user_id已经打过分的物品
    val filter_udf = udf { (items: Seq[String], items_v: Seq[String]) =>
      val fMap = items.map { x =>
        val l = x.split("_")
        (l(0), l(1))
      }.toMap
      items_v.filter { x =>
        val l = x.split("_")
        fMap.getOrElse(l(0), -1) == -1
      }
    }

    //    +-------+-------------------+--------------------+
    //    |user_id|                sim|       filtered_item|
    //    +-------+-------------------+--------------------+
    //    |    139|0.32266158985444504|[1017_2, 50_4, 76...|
    //    |    176|0.44033327143526596|[1017_2, 762_3, 2...|
    val df_filter_item = df_gen_item.withColumn("filtered_item", filter_udf(col("item_rating_arr"), col("item_rating_arr_v")))
      .select("user_id", "sim", "filtered_item")
    // 2.4 公式计算 相似度*rating
    val simRatingUDF = udf { (sim: Double, items: Seq[String]) =>
      items.map { x =>
        val l = x.split("_")
        l(0) + "_" + l(1).toDouble * sim
      }
    }

    val itemsimRating = df_filter_item.withColumn("item_prod", simRatingUDF(col("sim"), col("filtered_item")))
      .select("user_id", "item_prod")

    //    +-------+-------+------------------+
    //    |user_id|item_id|             score|
    //    +-------+-------+------------------+
    //    |    139|   1017|0.6453231797088901|
    //    |    139|     50|1.2906463594177802|
    val userItemScore = itemsimRating.select(itemsimRating("user_id"), explode(itemsimRating("item_prod")))
      .toDF("user_id", "item_prod")
      .selectExpr("user_id", "split(item_prod,'_')[0] as item_id", "cast(split(item_prod,'_')[1] as double) as score")
  }
}
