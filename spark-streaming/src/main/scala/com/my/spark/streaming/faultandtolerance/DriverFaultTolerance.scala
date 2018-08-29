package com.my.spark.streaming.faultandtolerance

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Driver端容错
  */
object DriverFaultTolerance {
  def main(args: Array[String]): Unit = {
    val checkpointPath = "hdfs://master:9999/user/bd-cqr/streaming/checkpoint"
    def createFunc():StreamingContext = {
      val conf = new SparkConf().setAppName("DriverFaultTolerance")
      val sc = new SparkContext(conf)
      val ssc = new StreamingContext(sc, Seconds(1))
      ssc.checkpoint(checkpointPath)
      //...流式处理逻辑
      ssc
    }
    val ssc = StreamingContext.getOrCreate(checkpointPath,createFunc _)
    ssc.start()
    ssc.awaitTermination()
  }

  
}
