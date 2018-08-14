package com.my.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 单词统计
  */
object WordCountLocal {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf()
    conf.setAppName("wordCount")
    conf.setMaster("local")

    val context = new SparkContext(conf)
    val textFile = context.textFile("src/main/resources/word.txt")
    val wordRDD = textFile.flatMap(_.split(" "))
    val pairWordRDD = wordRDD.map((_,1))
    val wordCountRDD = pairWordRDD.reduceByKey(_ + _)
    wordCountRDD.saveAsTextFile("src/main/resources/wordCountResult")
  }
}
