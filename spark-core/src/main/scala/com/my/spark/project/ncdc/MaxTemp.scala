package com.my.spark.project.ncdc

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 获取该年最高温度
  */
object MaxTemp {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("MaxTemp")
    if (args.size == 0) {
      conf.setMaster("local")
    }
    val ncdcPath = conf.get("spark.tracker.NcdcPath", "F:\\BigData\\workspace\\spark-demo\\spark-core\\src\\main\\resources\\")
    val spark = SparkSession.builder().appName("MaxTemp")
      .config(conf)
      .getOrCreate();
    val ncdcRecordPath = s"$ncdcPath/initedData/";
    val ncdcRecordDF = spark.read.parquet(ncdcRecordPath)
    //以sql的形式的访问数据
//    ncdcRecordDF.createOrReplaceTempView("NcdcRecord")
//    spark.sql("select max(maxTemp) from NcdcRecord").show()
    //以DataFrame Api的形式访问数据
   // val filterTempDF = ncdcRecordDF.filter($"maxTemp" > 21)
  }
}
