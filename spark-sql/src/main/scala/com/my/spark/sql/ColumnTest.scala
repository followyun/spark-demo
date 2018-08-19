package com.my.spark.sql

import com.my.spark.util.Utils._
import org.apache.spark.sql.SparkSession

/**
  * 列的相关api
  */
object ColumnTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ColumnTest").getOrCreate()
    val personDF = spark.read.json(s"${BASIC_PATH}/spark/dataset/person.json")
    personDF.select("name").show()
    //1. 获取所有的column
    personDF.columns
    //2. 构建单个column
    personDF.col("name")
    personDF("name")
    //3. $ - scala的构建column的简便符号
    import spark.implicits._
    personDF.select($"name").show

    //4. 从function中构建column
    import org.apache.spark.sql.functions._
    exp("age + 1")
    personDF.select(expr("age + 1")).show()
    lit("abc")
    personDF.select(lit("abc")).show()
    col("name")
    personDF.select(col("name")).show()
    column("name")
    personDF.select(column("name")).show()

    //dataframe增加一列
    personDF.withColumn("new_age", $"age").show()
    //给一列重命名
    personDF.withColumnRenamed("name", "new_name").show()
    //删除一列
    personDF.drop("name")
    //给多列重命名
    personDF.toDF("new_name", "new_age").show()
  }
}
