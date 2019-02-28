
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import com.huaban.analysis.jieba.{JiebaSegmenter, SegToken}
import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, udf}

object Kmeans {

  def main(args: Array[String]): Unit = {
    //    定义结巴分词类的序列化
    val conf = new SparkConf()
      .registerKryoClasses(Array(classOf[JiebaSegmenter]))
      .set("spark.rpc.message.maxSize", "800")
    //    建立sparkSession,并传入定义好的Conf
    val spark = SparkSession
      .builder()
      .appName("Jieba UDF")
      .enableHiveSupport()
      .config(conf)
      .getOrCreate()

    // 定义结巴分词的方法，传入的是DataFrame，输出也是DataFrame多一列seg（分好词的一列）
    def jieba_seg(df: DataFrame, colname: String): DataFrame = {
      val segmenter = new JiebaSegmenter()
      val seg = spark.sparkContext.broadcast(segmenter)
      val jieba_udf = udf { (sentence: String) =>
        val segV = seg.value
        segV.process(sentence.toString, SegMode.INDEX)
          .toArray().map(_.asInstanceOf[SegToken].word)
          .filter(_.length > 1).mkString(" ")
      }
      df.withColumn("seg", jieba_udf(col(colname)))
    }

    val df = spark.sql("select sentence from badou.news_seg limit 300")

    //    val df_seg = df.selectExpr("split(sentence,' ') as seg")
    //    调用udf进行结巴切词
    val df_seg = jieba_seg(df, "")

    //    实例化tf
    val TF = new HashingTF().setBinary(false)
      .setInputCol("seg")
      .setOutputCol("rawFeatures")
      .setNumFeatures(1 << 18)
    val df_tf = TF.transform(df_seg).select("rawFeatures", "label")

    //    对word进行idf加权
    val idf = new IDF()
      .setInputCol("rawFeatures")
      .setOutputCol("features")
      .setMinDocFreq(2)

    val idfModel = idf.fit(df_tf)
    val df_tfidf = idfModel.transform(df_tf).select("features", "label")

    //    实例化kmeans
    val kmeans = new KMeans()
      .setFeaturesCol("features")
      .setInitMode("k-means||") // 初始化K值模型
      .setInitSteps(5)
      .setK(10)
      .setMaxIter(20)
      .setPredictionCol("pred")
      .setSeed(2019)

    val model = kmeans.fit(df_tfidf)
    val WCSS = model.computeCost(df_tfidf)
    println(s"Set Sum of Squared Errors= $WCSS")
    //    打印中心点
    val dataFrame = model.transform(df_tfidf)
    dataFrame.count()

    println("cluster Centers:")

    val colum_connect = udf{df:DataFrame => "ksiemt"}

    df_tfidf.withColumn("label+", colum_connect(df_tfidf.col("label")))

    df_tfidf.describe()
    model.clusterCenters.length
    model.clusterCenters.map(v => v.toSparse).foreach(println)
    //    为什么中心点会有这么多值
    //    [0,0,0,1] [1,0,0,0] [0,2,0,0] [0,0,4,0]=> [0.25,0.5,1,0.25]
  }

}
