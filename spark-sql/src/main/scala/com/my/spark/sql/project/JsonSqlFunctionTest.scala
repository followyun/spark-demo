package com.my.spark.sql.project

import org.apache.spark.sql.{Row, SparkSession}
import com.my.spark.util.Utils.BASIC_PATH
import org.apache.spark.sql.types._

/**
  * json 函数使用
  *解析设备信息，并保存为json格式文件
  * get_json_object
  * from_json
  * to_json
  */
object JsonSqlFunctionTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("JsonSqlFunctionTest")
      .master("local").getOrCreate()

    import spark.implicits._
    val eventsRDD = spark.sparkContext.textFile(s"${BASIC_PATH}/spark/dataset/device_info.txt").map(line => {
      val data = line.split("::")
      Row(data(0).toLong, data(1))
    })

    val schema = StructType(StructField("id", LongType):: StructField("device", StringType)::Nil)

    val eventsDF = spark.createDataFrame(eventsRDD, schema)
    import org.apache.spark.sql.functions._

    //get_json_object 获取json部分属性
    //$.device_type: 取device 下的device_type属性
    //alias: 取别名
    eventsDF.select($"id", get_json_object($"device", "$.device_type").alias("device_type"), get_json_object($"device", "$.ip").alias("ip"),
      get_json_object($"device", "$.cca3").alias("cca3")).show()

    //from_json 转换json字符串为json对象
    val fieldSeq = Seq(StructField("battery_level", LongType), StructField("c02_level", LongType),
      StructField("cca3",StringType), StructField("cn", StringType),
      StructField("device_id", LongType), StructField("device_type", StringType),
      StructField("signal", LongType), StructField("ip", StringType),
      StructField("temp", LongType), StructField("timestamp", TimestampType))

    val jsonSchema = StructType(fieldSeq)
    val deviceDF = eventsDF.select(from_json($"device", jsonSchema) as "devices")
    deviceDF.show()

    deviceDF.select($"devices.*").filter($"devices.temp" > 10 and $"devices.signal" > 15).show()

    val devicesUSDF =
      deviceDF.select($"*").where($"cca3" === "USA").orderBy($"signal".desc, $"temp".desc)
    devicesUSDF.show()

    //to_json 获得json字符串
    val stringJsonDF = eventsDF.select(to_json(struct($"*"))).toDF("devices")
    stringJsonDF.printSchema()
    stringJsonDF.show()
  }
}
