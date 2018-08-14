package com.my.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 单词统计
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf()
    conf.setAppName("wordCount")

    val context = new SparkContext(conf)
    val textFile = context.textFile("hdfs://master:9999/user/bd-cqr/word.txt")
    val wordRDD = textFile.flatMap(_.split(" "))
    val pairWordRDD = wordRDD.map((_,1))
    val wordCountRDD = pairWordRDD.reduceByKey(_ + _)
    wordCountRDD.saveAsTextFile("hdfs://master:9999/user/bd-cqr/wordcount")
  }
}
