package com.my.spark.sql

import com.my.spark.util.Utils._
import org.apache.spark.sql.SparkSession

/**
  * 测试group相关的api
  */
object GroupApiTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]")
      .appName("DataPartitionTest").getOrCreate()
    import spark.implicits._

    val orderDF = spark.read.json(s"${BASIC_PATH}/spark/dataset/order_items.json")
    //基本聚合函数
    orderDF.groupBy("userId").count().show()
    orderDF.groupBy("userId").max().show()
    orderDF.groupBy("userId").min().show()
    orderDF.groupBy("userId").mean().show()
    orderDF.groupBy("userId").avg().show()
    //pivot 中心轴转换
    orderDF.groupBy("userId").pivot("name").sum("price").show()
//        +------+-----+----+-----+
//        |userId|apple|book| cake|
//        +------+-----+----+-----+
//        |     1| 20.0|10.0| null|
//        |     2| null|null|200.0|
//        +------+-----+----+-----+

    orderDF.groupBy("userId").pivot("name", Seq("apple", "cake")).sum("price").show()
//    +------+-----+-----+
//    |userId|apple| cake|
//    +------+-----+-----+
//    |     1| 20.0| null|
//    |     2| null|200.0|
//    +------+-----+-----+
    //聚合多个函数
    import org.apache.spark.sql.functions._

   orderDF.groupBy("userId").agg(
      avg("price"),
      max("price"),
      min("price")).show()

    orderDF.groupBy("userId").agg("price"->"avg"
    ,"price"->"max", "price"->"min").show() //==前一条agg

    val strDS = Seq("abc", "xyz", "hello").toDS()

    strDS.groupByKey(s => Tuple1(s.length)).count().show()

    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()

    ds.groupByKey(_._1).agg(sum("_2").as[Long]).show()

  }
}
