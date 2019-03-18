package com.badou

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileUtil
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}


object HdfsFile2Hive {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("test")
      .master("local[2]")
      .getOrCreate()

    val path = "hdfs://master:9000/data/raw_data"

    // 读取目录下所有的文件名存储在paths中
    def getHdfs(path: String) = {
      val conf = new Configuration()
      FileSystem.get(URI.create(path), conf)
    }

    def getFilesAndDirs(path: String): Array[Path] = {
      val fs = getHdfs(path).listStatus(new Path(path))
      FileUtil.stat2Paths(fs)
    }

    val sc = spark.sparkContext
    var paths = ListBuffer[String]()

    def listAllFiles(path: String) {
      val hdfs = getHdfs(path)
      val listPath = getFilesAndDirs(path)
      listPath.foreach(path => {
        if (hdfs.getFileStatus(path).isFile())
          paths.append(path.toString)
        else {
          listAllFiles(path.toString())
        }
      })
    }

    listAllFiles(path)
    //将读取文件的标题改为lable
    val matchLabel = (filePath: String) => filePath match {
      case file if file.contains("business") => "business"
      case file if file.contains("yule") => "yule"
      case file if file.contains("it") => "it"
      case file if file.contains("sports") => "sports"
      case file if file.contains("auto") => "auto"
    }
    //    val tem = paths.map(path => (path, matchLabel(path)))
    //获取(context,label,segs)形式的ListBuffer
    val listData = paths.map(filePath => (sc.textFile(filePath).collect().mkString("").replace(" ", ""),
      matchLabel(filePath), sc.textFile(filePath).collect().mkString("")))
    // 设置临时dataFrame结构
    val schemaString = "sentence label segs"
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    import spark.implicits._
    val frame = spark.read.text()
    frame.rdd.map(x=>x(0).toString).map(x=>x.split(","))
    val input = sc.parallelize(listData)
    //要将rdd转换成RowRdd才能使用spark.createDataFrame(rowRdd,schema)多参数的方法
    val rowRdd = input.map(x => Row(x._1, x._2, x._3))
    val result = spark.createDataFrame(rowRdd,schema)
    result.createOrReplaceTempView("temBlock")
    //插入到数据库
    spark.sql("select * from temBlock").write.mode(SaveMode.Overwrite).saveAsTable("news_noseg")
  }
}
