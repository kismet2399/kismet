package com.badou.exercise
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
    udata.rdd.map(x => (x(0).toString, x(2).toString))
    //3 使用udf时要引入
    import org.apache.spark.sql.functions._
    val dot_udf = udf((rateing: Int, rateing_v: Int) => rateing * rateing_v)
    //4 数据库可以存储集合
    // 使用udf的返回集合
    //5 使用udf一般集合withColumn使用
    val dot = udata.withColumn("dot", dot_udf(col("rating"), col("rating_v")))
    //6 列转行使用explode(其中col("itemsimRating")为数组或列表)
    val userItemScore = dot.select(dot("user_id"), explode(dot("itemsimRating")))
    //7 倒排使用
    userItemScore.orderBy(col("sum_score").desc)
    //8 map(x=(x(0),x(1))可以直接toMap
  }
}
