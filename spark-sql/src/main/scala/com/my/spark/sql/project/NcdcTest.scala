package com.my.spark.sql.project

import org.apache.spark.sql.SparkSession

/**
  * 气象站数据分析
  */
object NcdcTest {
  val spark = SparkSession.builder().master("local[*]")
    .appName("NcdcTest").getOrCreate()
  import spark.implicits._
  def main(args: Array[String]): Unit = {
    val ncdcDF = spark.read.load(s"hdfs://master:9999/user/bd-cqr/ncdc/initedData")
    val filterNcdcDF = ncdcDF .filter($"meanTemp" =!= 9999.9)
    filterNcdcDF.show()
    filterNcdcDF.createOrReplaceTempView("ncdc")
    import org.apache.spark.sql.functions._
    //平均温度
    filterNcdcDF.groupBy("year").agg(
      max("meanTemp"),
      avg("meanTemp")
    ).show()


  }

}
