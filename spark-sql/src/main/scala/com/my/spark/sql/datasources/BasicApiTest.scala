package com.my.spark.sql.datasources
import com.my.spark.util.Utils._

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 测试读写外部数据源的基本api load和save
  *parquet, json, csv, text, table(spark sql, hive独有的), orc, jdbc
  */
object BasicApiTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]")
      .appName("BasicApiTest").getOrCreate()

    //load读取文件，默认读取parquet文件
    val trackerSessionDF = spark.read.load(s"${BASIC_PATH}/spark/userbehaviour/trackerSession")
    val trackerSessionDF1 = spark.read.parquet(s"$BASIC_PATH/spark/userbehaviour/trackerSession") //== spark.read.load
    val trackerSessionDF2 = spark.read.format("parquet").load(s"$BASIC_PATH/spark/userbehaviour/trackerSession")// == spark.read.parquet
    trackerSessionDF.show()

    //读取多个文件
    val tSAndTLDF = spark.read.load(s"$BASIC_PATH/spark/userbehaviour/trackerSession",
      s"$BASIC_PATH/spark/userbehaviour/trackerLog")

    //option传参数，改变读写数据源的行为
    spark.read.option("mergeSchema", "true").parquet(s"$BASIC_PATH/spark/userbehaviour/trackerSession")

    //使用map设置多个参数
    val optsMap = Map("mergeSchema" -> "mergeSchema")
    spark.read.options(optsMap).parquet("")
    //保存文件
    trackerSessionDF.write.save(s"$BASIC_PATH/spark/userbehaviour/trackerSession_new")
    trackerSessionDF.write.option("compression", "snappy").save(s"$BASIC_PATH/spark/userbehaviour/trackerSession_new")

    //保存文件几种模式
    /**
      * SaveMode.Append == "append"
     * SaveMode.Overwrite == "overwrite"
     * SaveMode.ErrorIfExists == "error"   //默认
     * SaveMode.Ignore == "ignore"
      */
    trackerSessionDF.write.mode(SaveMode.ErrorIfExists).save(s"$BASIC_PATH/spark/userbehaviour/trackerSession_new")
    trackerSessionDF.write.mode("error").save(s"$BASIC_PATH/spark/userbehaviour/trackerSession_new") //等同以上
  }

}
