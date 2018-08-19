package com.my.spark.sql

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
  */
object DatasetTest {
  private val logger = LoggerFactory.getLogger("Dataset")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Dataset Test").getOrCreate()
    val df = spark.read.json("hdfs://master:9999/user/bd-cqr/person.json")
    df.schema
    df.rdd
    import spark.implicits._
    df.createOrReplaceTempView("person")
    val sqlDF = spark.sql("select * from person where arge > 10") //以sql的形式查询数据
    sqlDF.show()

    val filterAgeDf = df.filter($"age" > 10) //以DataFrame api的方式查询数据
    filterAgeDf.show()

    val personDF = spark.read.json("hdfs://master:9999/user/bd-cqr/person.json")
    //Dataset的强类型
    val personDS = personDF.as[Person]
    val primitiveDS = Seq(1, 2, 3).toDS()

    //Dataset支持强大的lambda表达式
    val filterDS = personDS.filter(person => {
      if (person.age.isDefined && person.age.get > 20) {
        true
      } else {
        logger.info(s"========= filter ${person.age}")
        false
      }
    })

    filterDS.collect()

    //DataFrame为类型为Row的Dataset,即Dataset[Row]
    //可以将Dataset理解为带schema的RDD

  }
}

case class Person(name: String, age: Option[Long])
