package com.my.spark.streaming.recevier

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 接收flume发来的数据
  * 提交streaming程序
  * spark-submit \
  * --class com.my.spark.streaming.recevier.FlumeStream \
  * --master spark://master:7077 \
  * --deploy-mode client \
  * --driver-memory 512m \
  * --executor-memory 512m \
  * --total-executor-cores 4 \
  * --executor-cores 2 \
  * ~/mr-course/spark-streaming-1.0-SNAPSHOT-jar-with-dependencies.jar
  */
object FlumePushStream {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FlumeStream")
    val sc = new SparkContext(conf)

    //创建spark streaming编程入口，处理间隔为1S
    val ssc = new StreamingContext(sc, Seconds(1))
    //创建一个flume接收器，接收flume发送过来的数据
    val stream = FlumeUtils.createStream(ssc, "192.168.112.131", 9998, StorageLevel.MEMORY_AND_DISK)
    stream.count.map(cn => "recive "+cn +" events").print()
    //启动streaming应用
    ssc.start()

    //等待程序运行结束
    ssc.awaitTermination()
  }
}
