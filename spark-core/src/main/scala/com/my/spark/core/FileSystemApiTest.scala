package com.my.spark.core

import java.sql.{DriverManager, ResultSet}

import org.apache.avro.ipc.specific.Person
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.mapreduce.lib.input.{TextInputFormat => NewTextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{TextOutputFormat => NewTextOutputFormat}
import org.apache.spark.rdd.JdbcRDD

/**
  * 文件系统读写的测试
  */
object FileSystemApiTest {
  val conf = new SparkConf().setAppName("FileSystemApiTest")
  val sc = new SparkContext(conf)
  val spark = SparkSession.builder().appName("FileSystemApiTest")
    .config("spark.some.config.option", "some-value")
    .getOrCreate();
  import spark.implicits._
  def main(args: Array[String]): Unit = {

  }

  /**
    * 测试hdfs， 本地，s3中读写文件
    */
  def test1(): Unit ={
    sc.textFile("file://")//本地磁盘中读取文件
    val paralRDD = sc.makeRDD(Seq(1,23,4,5), 2)

    //old hdfs api
    val hdfsRDD = sc.hadoopFile("hdfs://", classOf[TextInputFormat], classOf[LongWritable], classOf[Text])
    paralRDD.saveAsTextFile("hdfs://xxx")

    //new hdfs api
    sc.newAPIHadoopFile("hdfs://", classOf[NewTextInputFormat], classOf[LongWritable], classOf[Text])
    hdfsRDD.saveAsNewAPIHadoopFile[NewTextOutputFormat[LongWritable, Text]]("hdfs")

    //jdbc
    def createConnection() = {
        Class.forName("com.mysql.jdbc.Driver")
      DriverManager.getConnection("jdbc:mysql://xxx")
    }

    def extractValue(r:ResultSet)={
      (r.getInt(1), r.getString(2))
    }

    val sql = "select * from xxx"

    val dataJdbcRDD = new JdbcRDD(sc, createConnection, sql, lowerBound = 1, upperBound = 3,
      numPartitions = 2, mapRow = extractValue)
    dataJdbcRDD.collect

    //s3
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "YOUR_KEY_ID")
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "YOUR_SECRET")
    paralRDD.saveAsTextFile("s3n://bucket/test")
    val s3FileInput = sc.textFile("s3n://bucket/*.log")
    s3FileInput.collect()

  }

  /**
    * 测试读写json，parquet，avro文件
    */
  def test2(): Unit ={
    //val personDF = spark.sparkContext.parallelize(Seq(Person("jeffy", 30), Person("tom", 23), Person("tomy", 1)), 2).toDF()
    //json
//    personDF.write.mode(SaveMode.Overwrite).json("hdfs://")
//    val jsonDF = spark.read.json("hdfs://xxx")
//    jsonDF.show()
//
//    //avro
//    spark.read.avro("hdfs://")
//    personDF.write.mode(SaveMode.Overwrite).avro("hdfs://")
//    //parquet
//    spark.read.parquet("hdfs://")
//    personDF.write.mode(SaveMode.Overwrite).parquet("hdfs://")

  }
}
