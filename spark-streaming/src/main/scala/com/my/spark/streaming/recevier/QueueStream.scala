package com.my.spark.streaming.recevier

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 消息队列接收器
  */
object QueueStream {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("QueueStream")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))
    val queue = new mutable.Queue[RDD[Int]]()
    val inputStream = ssc.queueStream(queue)
   val mappedStream =  inputStream.map(x => (x % 10, 1))
    val reducedStream = mappedStream.reduceByKey(_+_)
    reducedStream.print()

    ssc.start()
    ssc.awaitTermination()
    //添加数据到queue中
    queue += ssc.sparkContext.makeRDD(1 to 100, 5)
  }
}
