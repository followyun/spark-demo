package com.my.spark.sql.datasources

import org.apache.spark.sql.SparkSession
import com.my.spark.util.Utils._

/**
  */
object TableTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]")
      .appName("TableTest").getOrCreate()

    println(spark.conf.get("spark.sql.catalogImplementation"))//打印table的类型

    val personDF = spark.read.json(s"${BASIC_PATH}/spark/dataset/person.json")

    //保存为表
    personDF.write.saveAsTable("person")
    //查看有哪些表
    spark.catalog.listTables().show()

    //读取表中数据
    val personTable = spark.read.table("person")
    personTable.select("name").show()

    //创建表视图
    personDF.createOrReplaceTempView("person_tv")
    spark.read.table("person_tv").show()
    //用sql的方式查看表中内容
    spark.sql("select age from person_tv").show()

    //创建外部表
    spark.catalog.createTable("trackerSession_outer", s"${BASIC_PATH}/spark/userbehaviour/trackerSession")

    //读取指定的格式的外部数据创建表
    spark.catalog.createTable("person_json", s"${BASIC_PATH}/spark/dataset/people.json", "json")

    //删除外部表数据不会被删除
    spark.sql("drop table trackerSession_outer")
    //创建内部表
    spark.catalog.createTable("trackerSession_inner")//默认在spark.catalog.currentDatabase下
    //删除内部表数据会被删除
    spark.sql("drop table trackerSession_inner")

    val sessionDF = spark.read.parquet(s"${BASIC_PATH}/spark/userbehaviour/trackerLog")
    //创建全局表
    sessionDF.createGlobalTempView("global_session")
    //访问全局表必须加上前缀global_temp，否则查找不到该表
    //global_temp是spark sql保留的数据库，全局临时表都放在这里面
    spark.sql("select * from global_temp.global_session").show

    spark.stop()
  }
}
