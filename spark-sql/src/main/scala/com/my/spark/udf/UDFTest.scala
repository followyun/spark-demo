package com.my.spark.udf

import com.my.spark.sql.User
import com.my.spark.util.Utils.BASIC_PATH
import org.apache.spark.sql.SparkSession

/**
  * spark sql 用户自定义函数
  */
object UDFTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]")
      .appName("DataPartitionTest").getOrCreate()
    import spark.implicits._

    val usersDS = spark.read.json(s"${BASIC_PATH}/spark/dataset/users.json").repartition(2).as[User]
    //调用自定义typed函数
    usersDS.select(TypedAverage.toColumn).show

    usersDS.toDF.createOrReplaceTempView("user")
    spark.udf.register("myAverage", UntypedAverage)
    //调用自定义非typed函数
    spark.sql("select myAverage(age) from user").show()
  }
}
