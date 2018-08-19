package com.my.spark.sql.datasources

import org.apache.spark
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.my.spark.util.Utils._

/**
  * text文件读写
  */
object TextFileTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]")
      .appName("TextFileTest").getOrCreate()
    //1: 将json文件数据转化成text文件数据
    val df = spark.read.json(s"${BASIC_PATH}/people.json")
    df.write.mode(SaveMode.Overwrite).text(s"${BASIC_PATH}/text") //报错
    df.select("age").write.mode(SaveMode.Overwrite).text(s"${BASIC_PATH}/text") //报错, 只支持写入string类型的数据

    //compression
    //`none`, `bzip2`, `gzip`
    //todo 支持哪些压缩格式呢？
    df.select("name").write.mode(SaveMode.Overwrite).option("compression", "bzip2").text(s"${BASIC_PATH}/text")

    //读取text文件，返回DataFrame
    val textDF = spark.read.text(s"${BASIC_PATH}/text")
    textDF.show()
    //读取text文件，返回Dataset
    val textDs = spark.read.textFile(s"${BASIC_PATH}/text")
    textDs.show()
  }
}
