package com.my.spark.streaming.recevier

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 文件接收器
  * 监控的目录下的文件格式必须是统一的
  * 不支持嵌入文件目录
  * 一旦文件移动到监控目录下是不能变的，追加文件内容并不会被读取
  */
object HDFSFileStream {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("QueueStream")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))
    val filePath = s"hdfs://master:9999/user/bd-cqr/streaming/"
    //(path:Path) => path.toString.contains("process") 为文件过滤函数，只处理包含名字process的文件
    val wordsDataStream = ssc.fileStream[LongWritable, Text, TextInputFormat](filePath,
      (path: Path) => path.toString.contains("process"), true).map(_._2.toString)
    val words = wordsDataStream.flatMap(_.split(" "))
    val wordsPairs = words.map((_, 1))
    val wordsCount = wordsPairs.reduceByKey(_ + _)
    wordsCount.print()
    ssc.start()
    ssc.awaitTermination()
    //添加数据到queue中
  }
}
