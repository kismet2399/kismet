package com.badou.lr

import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object LRSelf {
  def main(args: Array[String]): Unit = {
    //获取订单数据,历史和train数据
    val spark = SparkSession
      .builder()
      .appName("LR")
      .enableHiveSupport()
      .getOrCreate()

    val orders = spark.sql("select * from orders")
    val priors = spark.sql("select * from order_products_prior")
    val trains = spark.sql("select * from trains")

    //创建训练集数据,trains的为1,priors的为0
    import spark.implicits._
    val or = trains.join(orders, "order_id").select("user_id", "product_id").distinct()
    val op = priors.join(orders, "order_id").select("user_id", "product_id").distinct().withColumn("label", lit(1))
    val trainData = op.join(or, Seq("user_id", "product_id"), "outer").na.fill(0)
    //获取特征值
    val (prodFeat, userFeat) = SimpleFeature.Feat(priors, orders)

    //获取训练数据
    val train = trainData.join(prodFeat, "product_id").join(userFeat, "user_id")

    //模型训练
    /**
      * 每个用户平均购买订单的间隔周期:u_avg_day_gap
      * 每个用户的总订单数:user_ord_cnt
      * 用户购买商品种类:u_prod_dist_cnt
      * 用户订单中商品个数的平均值:u_avg_ord_prods
      * 商品被再次购买的数量:prod_sum_rod
      * 商品被再次购买率:prod_rod_rate
      * 商品销售量:prod_cnt
      */
    //    特征处理通过rformula:离散化特征one-hot，连续特征不处理，
    //    最后将分别处理的特征向量拼成最后的特征
    val rFormula = new RFormula().setFormula("label ~ u_avg_day_gap +user_ord_cnt +" +
      " u_prod_dist_cnt+u_avg_ord_prods+prod_sum_rod+prod_rod_rate+prod_cnt")
      //设置特征参数列名
      .setFeaturesCol("features")
      //设置结果参数
      .setLabelCol("label")
    val df = rFormula.fit(train).transform(train).select("features","label")
    //  算法为收敛，迭代停止是因为达到了做大迭代次数
    // LogisticRegression training finished but the result is not converged because: max iterations reached
    // 创建LR模型
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0)
    // 模型训练
    val model = lr.fit(df)
    // 打印权重系数(w)和截距b
    print(s"coefficients: ${model.coefficients}<==>intercept: ${model.intercept}")

    val summary = model.summary
    val history = summary.objectiveHistory
    //打印损失函数
    history.foreach(loss=> println(loss))
    val binarySummary = summary.asInstanceOf[BinaryLogisticRegressionSummary]
    val roc = binarySummary.roc
    //TPR,FPR
    roc.show()
    //AUC
    println(binarySummary.areaUnderROC)
  }
}
