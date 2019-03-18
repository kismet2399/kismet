package mllib

import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}


//二分类
object SVMwithSGD {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
    val sql  = new SQLContext(sc);
    val data: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "svm.txt")
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)
    //        data.foreach( x => println(x.toString()))
    //        data.foreach( x => println(x.label))
    data.foreach( x => println(x.features))
    val numIterations = 100
    val model: SVMModel = SVMWithSGD.train(training, numIterations)
    model.clearThreshold()//为了模型拿到评分 不是处理过之后的分类结果

    val scoreAndLabels: RDD[(Double, Double)] = test.map { point =>
      //                大于0 小于0 两类
      val score = model.predict(point.features)
      (score, point.label)
    }

    scoreAndLabels.foreach(println)

  }

}