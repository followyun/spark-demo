package com.my.spark.sql.datasources

import org.apache.spark.sql.SparkSession
import com.my.spark.util.Utils._

/**
  * catalog（元数据） api测试
  * spark-shell --master spark://master:7077 --conf spark.sql.catalogImplementation=in-memory
  */
object CatalogApiTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]")
      .appName("DataPartitionTest").getOrCreate()

    //查看所有的数据库
    spark.catalog.listDatabases().show()
    //创建数据库
    spark.sql("CREATE DATABASE IF not exists test " +"COMMENT 'Test database' Location 'hdfs://master:9999/user/bd-cqr/spark/spark-db'")
    //查看所有的表
    spark.catalog.listTables().show()
    //创建表
    val sessionDF = spark.read.parquet(s"${BASIC_PATH}/spark/userbehaviour/trackerLog")
    sessionDF.write.saveAsTable("session")
    //创建临时表
    sessionDF.createOrReplaceTempView("temp_session")
    //查看表中字段信息
    spark.catalog.listColumns("temp_session").show()

    //缓存表
    spark.catalog.cacheTable("temp_session")
    //取消缓存
    spark.catalog.uncacheTable("temp_session")
    //删除数据库
    spark.sql("drop database test")
    //删除表
    spark.sql("drop table session")

    spark.sql("select * from session").show()
  }
}
