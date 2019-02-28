package com.badou.kmeans

import com.badou.kmeans.SimpleFeature
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object LRTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("LR")
      .enableHiveSupport()
      .getOrCreate()

    val orders = spark.sql("select * from badou.orders")
    val priors = spark.sql("select * from badou.order_products_prior")
    val trains = spark.sql("select * from badou.trains")

    val orderFeat = orders.selectExpr("order_id",
      "cast(order_hour_of_day as int) as order_hour_of_day",
      "cast(days_since_prior_order as double) as days_since_prior_order").na.fill(0)

    val (prodFeat,userFeat,xFeat) = SimpleFeature.Feat(priors,orders)
    /**
      * 数据集分析
      */
//    //    prior,train,test用户集合的情况分析 结论：train+test=prior
//    //    131209
//    val train_user = orders.filter("eval_set='train'").select("user_id").distinct()
////    75000
//    val test_user = orders.filter("eval_set='test'").select("user_id").distinct()
////    206209
//    val prior_user = orders.filter("eval_set='prior'").select("user_id").distinct()
////    0
//    val interset_user = train_user.intersect(test_user)
////    131209
//    val train_inter = prior_user.intersect(train_user)
////    75000
//    val test_inter = prior_user.intersect(test_user)

    val op = orders.join(priors,"order_id") //eval_set= prior
    val optrain = orders.join(trains,"order_id") //eval_set = train
    import spark.implicits._

//  prior去重样本数量：13307953
//    val user_recall = op.select("user_id","product_id").distinct()
//    user_real与user_recall的交集是828823，user_real数据集：1384616
    val user_real = optrain.select("product_id","order_id").distinct()
      .withColumn("label",lit(1))
//    正样本：1384616 ，负样本：12479130
    val trainData = op.join(user_real,Seq("product_id","order_id"),"outer")
      .select("user_id","product_id","order_id").na.fill(0)

    val upHourVsLast = udf{(ordHour:Int,lastOrderHour:Int)=>
      val gap = ordHour - lastOrderHour
      math.min(gap,24-gap)
    }

    val train = trainData.join(userFeat,"user_id")
      .join(prodFeat,"product_id")
      .join(orderFeat,"order_id")
      .selectExpr("*","days_since_prior_order / cast(u_avg_day_gap as double) as days_since_ratio")
      .join(xFeat,Seq("user_id","product_id"),"left_outer").na.fill(-1)
      .selectExpr("*","(user_ord_cnt -max_ord_num) as up_orders_since_last")
      .withColumn("up_hour_vs_last",upHourVsLast(col("order_hour_of_day"),col("last_order_hour")))


//  模型

// 具体特征：Array(product_id, user_id, label, u_avg_day_gap, user_ord_cnt, u_prod_dist_cnt,
// u_prod_records, u_avg_ord_prods, prod_sum_rod, prod_rod_rate, prod_cnt)
//    特征处理通过rformula:离散化特征one-hot，连续特征不处理，
//    最后将分别处理的特征向量拼成最后的特征
    val rformula = new RFormula()
  .setFormula("label ~ u_avg_day_gap +user_ord_cnt + u_prod_dist_cnt" +
  "+u_avg_ord_prods+prod_sum_rod+prod_rod_rate+prod_cnt + days_since_ratio + " +
    "up_hour_vs_last + order_hour_of_day + days_since_prior_order + avg_pos_in_cart + orders_cnt")
  .setFeaturesCol("features")
  .setLabelCol("label")

//    对数据进行rformula处理生成新的数据：features是特征向量，label是标签
    val df = rformula.fit(train).transform(train).select("features","label")
//      .cache()
//  算法为收敛，迭代停止是因为达到了做大迭代次数
// LogisticRegression training finished but the result is not converged because: max iterations reached
//    lr模型的定义
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0)

//    划分训练集和测试集
    val Array(trainingData,testData)=df.randomSplit(Array(0.7,0.3))
//    模型训练
    val lrModel = lr.fit(trainingData)
    // 打印系数（weight：W）和截距b
    print(s"Coefficients: ${lrModel.coefficients} intercept: ${lrModel.intercept}")

    val trainingSummary = lrModel.summary
    val objectHistory = trainingSummary.objectiveHistory
//   打印loss
    objectHistory.foreach(loss=>println(loss))
//    0.30699099073008507

    val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]

    val roc = binarySummary.roc
//   TPR,FPR
    roc.show()
//    AUC 0.6809456382773528
    println(binarySummary.areaUnderROC)

//    预测testData
    val test = lrModel.transform(testData)

  }

}
