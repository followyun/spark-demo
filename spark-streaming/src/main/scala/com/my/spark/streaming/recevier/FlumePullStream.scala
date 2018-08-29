package com.my.spark.streaming.recevier

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 提取flume avro server上的数据
  * 提交streaming程序
  * spark-submit \
  * --class com.my.spark.streaming.recevier.FlumePullStream \
  * --master spark://master:7077 \
  * --deploy-mode client \
  * --driver-memory 512m \
  * --executor-memory 512m \
  * --total-executor-cores 4 \
  * --executor-cores 2 \
  * ~/mr-course/spark-streaming-1.0-SNAPSHOT-jar-with-dependencies.jar
  */
object FlumePullStream {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FlumeStream")
    val sc = new SparkContext(conf)
    if(args.length < 2){
      println("need [host(args(0)] and [port(args(1))]!")
    }
    val host = args(0)
    val port = args(1).toInt
    //创建spark streaming编程入口，处理间隔为1S
    val ssc = new StreamingContext(sc, Seconds(1))
    //创建一个flume接收器，接收flume发送过来的数据
    val stream = FlumeUtils.createPollingStream(ssc, host, port.toInt, StorageLevel.MEMORY_AND_DISK)
    stream.count.map(cn => "recive "+cn +" events").print()
    //启动streaming应用
    ssc.start()

    //等待程序运行结束
    ssc.awaitTermination()
  }
}
