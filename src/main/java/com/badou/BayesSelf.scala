package com.badou

import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import com.huaban.analysis.jieba.{JiebaSegmenter, SegToken}
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, IDF, StringIndexer}
import org.apache.spark.sql.SparkSession

/**
  * 1,获取标签:context的RDD-->label:sentence
  * 2,对使用HashingTF对sentence中word进行编码和词频统计
  * 3,对word进行IDF加权
  * 4,对label进行编码将String转化成Double
  * 5,使用bayes模型进行训练,预测
  * 6,模型评估
  */
object BayesSelf {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .registerKryoClasses(Array(classOf[JiebaSegmenter])) //通过JiebaSegmenter序列化类来优化
      .set("spark.rpc.message.maxSize", "800")

    val spark = SparkSession.builder()
      .appName("kismet")
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    //1,定义结巴分词方法,将一列句子切分成分词后的数组,列名为seg
    import org.apache.spark.sql.functions._
    val jiebaSeg = udf { (sentence: String) =>
      val seger = new JiebaSegmenter
      seger.process(sentence, SegMode.INDEX)
        .toArray().map(_.asInstanceOf[SegToken].word)
    }

    val df = spark.sql("select * from news_noseg")
    // 调用jieba分词进行分词
    //    val df_seg = df.withColumn("seg",jiebaSeg(col("sentence")))

    val df_seg = df.selectExpr("label", "split(segs,' ')as seg")
    // 2,对word进行hash编码和统计
    val tfModel = new HashingTF()
      .setBinary(false) //使用多项式而不是伯努利方式
      .setInputCol("seg")
      .setOutputCol("rawFeatures")

    //编码
    val df_tf = tfModel.transform(df_seg).select("label", "rawFeatures")

    // 3,对word进行IDF加权
    val idfSchem = new IDF()
      .setInputCol("rawFeatures")
      .setOutputCol("features")
      .setMinDocFreq(1) //低于多少频率不统计

    val idfModel = idfSchem.fit(df_tf)
    //必须先统计总的单词数才能计算idf
    val tfIdf = idfModel.transform(df_tf).select("label", "features")

    // 4,对label进行编码将String转化成Double
    val stringIndex = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexs")
      .setHandleInvalid("error")

    val trainData = stringIndex.fit(tfIdf).transform(tfIdf)
    val Array(train, test) = trainData.randomSplit(Array(0.8, 0.2))
    // 5,使用bayes模型进行训练,预测
    val nb = new NaiveBayes()
      .setModelType("multinomial")//使用多项式类型进行训练
      .setSmoothing(1)//设置平滑参数中的默认频次
      .setFeaturesCol("features")
      .setLabelCol("indexs")
      .setPredictionCol("preIndexs")
      .setProbabilityCol("probIndexs")
      .setRawPredictionCol("rawPred")

    val bayesModel = nb.fit(train)
    val pred = bayesModel.transform(test)
    pred.filter("indexs != preIndexs").select("probIndexs","rawPred").show(2,false)
    //  6,模型评估
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexs")
      .setPredictionCol("preIndexs")
      //    param for metric name in evaluation (supports `"f1-->2pr(p+r)"` (default),
      //    `"weightedPrecision-->精确率"`,`"weightedRecall-->召回率"`, `"accuracy-->准确率"`)
      .setMetricName("weightedRecall")
    evaluator.evaluate(pred)


  }
}
