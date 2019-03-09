package com.badou.offline

import com.huaban.analysis.jieba.{JiebaSegmenter, SegToken}
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import scala.io.Source

object JiebaKry {
  def main(args: Array[String]): Unit = {
//    定义结巴分词类的序列化
    val conf = new SparkConf()
      .registerKryoClasses(Array(classOf[JiebaSegmenter]))
      .set("spark.rpc.message.maxSize","800")
//    建立sparkSession,并传入定义好的Conf
    val spark = SparkSession
      .builder()
      .appName("Jieba UDF")
      .enableHiveSupport()
      .config(conf)
      .getOrCreate()

    // 定义结巴分词的方法，传入的是DataFrame，输出也是DataFrame多一列seg（分好词的一列）
    def jieba_seg(df:DataFrame,colname:String): DataFrame ={
      val segmenter = new JiebaSegmenter()
      val seg = spark.sparkContext.broadcast(segmenter)//broadcast广播变量(分发到每个节点)
      val jieba_udf = udf{(sentence:String)=>
        val segV = seg.value
        segV.process(sentence.toString,SegMode.INDEX)
          .toArray().map(_.asInstanceOf[SegToken].word)
          .filter(_.length>1).mkString("/")
      }
      df.withColumn("seg",jieba_udf(col(colname)))
    }

    // val df =spark.sql("select sentence,label from badou.news_noseg limit 300")
    val df = spark.sql("select * from news_noseg")
    df.select("segs").rdd.map(x=>x.toString()).map(_.split(" ")).flatMap(_.map((_,1))).reduceByKey(_+_).sortBy(_._2,false).collect()
    val df_seg = jieba_seg(df,"sentence")
    df_seg.show()
    df_seg.write.mode("overwrite").saveAsTable("badou.news_jieba")
    val lines = Source.fromFile("./The_man_of_property.txt").getLines().toList
  }
}
