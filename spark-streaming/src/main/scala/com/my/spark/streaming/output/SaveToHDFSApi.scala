package com.my.spark.streaming.output

import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 保存streaming计算结果到hdfs中
  */
object SaveToHDFSApi {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("NetWorkWordCount")
    val sc = new SparkContext(conf)

    //创建spark streaming编程入口，处理间隔为1S
    val ssc = new StreamingContext(sc, Seconds(1))
    //创建一个接收器，接收master服务器发送过来的数据
    val lines = ssc.socketTextStream("master", 9998, StorageLevel.MEMORY_AND_DISK_SER_2)

    val words = lines.flatMap(_.split(" "))
    val wordsPairs = words.map((_, 1))
    val wordsCount = wordsPairs.reduceByKey(_ + _)
    //将数据保存到HDFS中
    val wordsText = wordsCount.repartition(1).map(value => {
      val text = new Text()
      text.set(value.toString())
      (NullWritable.get(), text)
    })
    wordsText.saveAsHadoopFiles[TextOutputFormat[NullWritable, Text]]("hdfs://master:9999/user/bd-cqr/streaming/output/wordcount/", "wordcount")

    //或如下api
    wordsText.saveAsTextFiles("hdfs://master:9999/user/bd-cqr/streaming/output/wordcount/text", "wordcount")
    wordsText.saveAsObjectFiles("hdfs://master:9999/user/bd-cqr/streaming/output/wordcount/object", "wordcount")
    //启动streaming应用
    ssc.start()

    //查看结果 coalesce方法为将多个文件合并为一个文件
    sc.textFile("hdfs://master:9999/user/bd-cqr/streaming/output/wordcount/text*").coalesce(1).collect
    //等待程序运行结束
    ssc.awaitTermination()

    ssc.stop(false) //参数为是否关闭sparkcontext， 默认为关闭
  }
}
