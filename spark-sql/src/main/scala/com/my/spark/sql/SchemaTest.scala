package com.my.spark.sql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

/**
  * schema的使用
  */
object SchemaTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SchemaApiTest")
      .master("local")
      .getOrCreate()
    //2: schema中可以有复杂数据类型
    val schema =
      StructType(
        StructField("name", StringType, false) ::
          StructField("age", IntegerType, true) ::
          StructField("map", MapType(StringType, StringType), true) ::
          StructField("array", ArrayType(StringType), true) ::
          StructField("struct",
            StructType(Seq(StructField("field1", StringType), StructField("field2", StringType))))
          :: Nil)

    val people =
      spark.sparkContext.parallelize(Seq("tom,30", "katy, 46")).map(_.split(",")).map(p =>
        Row(p(0), p(1).trim.toInt, Map(p(0) -> p(1)), Seq(p(0), p(1)), Row("value1", "value2")))
    val dataFrame = spark.createDataFrame(people, schema)
    dataFrame.printSchema
    dataFrame.show()

    dataFrame.select("map").collect().map(row => row.getAs[Map[String, String]]("map"))
    dataFrame.select("array").collect().map(row => row.getAs[Seq[String]]("array"))
    dataFrame.select("struct").collect().map(row => row.getAs[Row]("struct"))
  }
}
