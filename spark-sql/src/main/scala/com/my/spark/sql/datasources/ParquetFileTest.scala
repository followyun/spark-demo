package com.my.spark.sql.datasources

import com.my.spark.util.Utils._
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * parquet文件读写测试
  */
object ParquetFileTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]")
      .appName("BasicApiTest").getOrCreate()
    val personDF = spark.read.json(s"${BASIC_PATH}/spark/dataset/person.json")
    personDF.write.save(s"${BASIC_PATH}/spark/dataset/json")
    val renameSchemaDF = personDF.toDF("new_age", "name")
    renameSchemaDF.write.mode(SaveMode.Overwrite).parquet(s"${BASIC_PATH}/spark/dataset/json_new")
    //"mergeSchema合并多个文件中的schema"
    val mergeDF = spark.read.option("mergeSchema", "true").parquet(s"${BASIC_PATH}/spark/dataset/json", s"${BASIC_PATH}/spark/dataset/json_new")
    mergeDF.show()
  }
}
