package com.badou.exercise

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object template {
  def kismet(): Unit = {
    val spark = SparkSession.builder()
      .appName("just kismet")
      .enableHiveSupport()
      .getOrCreate()
    val udata = spark.sql("select * from udata")
    //1 rdd转dataFrame需要引入
    import spark.implicits._
    //2 转成rdd时数据都处理成String,这样在处理时可以直接转换(toDouble,toInt)
    udata.rdd.map(x => (x(0).toString, x(2).toString)).map(x => x._1.toDouble)

    //3 使用udf时要引入
    import org.apache.spark.sql.functions._
    val kismet_udf = udf { (col1: Int, col2: Int) => List[Int](col1 ,col2) }
    val dot_udf = udf((rateing: Int, rateing_v: Int) => rateing * rateing_v)
    //4 数据库可以存储集合
    // 使用udf的返回集合
    //5 使用udf一般集合withColumn使用
    val dot = udata.withColumn("dot", kismet_udf(col("rating"), col("rating")))
    //6 列转行使用explode(其中col("itemsimRating")为数组或列表)
    val userItemScore = dot.select(dot("user_id"), explode(dot("dot"))as("dot_list"))
    //7 倒排使用
    userItemScore.orderBy(col("sum_score").desc)
    //8 map(x=(x(0),x(1))可以直接toMap
    //9 lit给统一的值要引入(增加一行lable值为1
    userItemScore.withColumn("label", lit(1))
    //10 启动线程数，至少是两个。一个线程用于监听数据源，其他线程用于消费或打印。至少是2个
    val conf = new SparkConf().setMaster("local[5]").setAppName("kafkainput")
    //11 rdd的排序中这是false即为倒叙
    udata.rdd.map(x => (x(0).toString, x(2).toString)).map(x => (x._1, x._2.toDouble)).sortBy(_._2,false)
  }
}
