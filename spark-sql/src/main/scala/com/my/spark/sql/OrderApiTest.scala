package com.my.spark.sql

import com.my.spark.util.Utils.BASIC_PATH
import org.apache.spark.sql.SparkSession
import com.my.spark.util.Utils._

/**
  * 排序测试
  */
object OrderApiTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]")
      .appName("DataPartitionTest").getOrCreate()
    import spark.implicits._

    val usersDS = spark.read.json(s"${BASIC_PATH}/spark/dataset/users.json").repartition(2).as[User]
    usersDS.orderBy($"age".desc) //倒序排序
    usersDS.sort("age")

    //分区内进行排序
    usersDS.sortWithinPartitions($"age").rdd.glom().collect()
  }
}
