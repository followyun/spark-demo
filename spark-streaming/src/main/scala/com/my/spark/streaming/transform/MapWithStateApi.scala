package com.my.spark.streaming.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._


/**
  * 跟UpdateStateByKey功能相似，但是性能提高了5到10倍
  */
object MapWithStateApi {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("NetWorkWordCount")
    val sc = new SparkContext(conf)

    //创建spark streaming编程入口，处理间隔为1S
    val ssc = new StreamingContext(sc, Seconds(1))
    ssc.checkpoint("hdfs://master:9999/user/bd-cqr/streaming/checkpoint")
    //创建一个接收器，接收master服务器发送过来的数据

    val lines = ssc.socketTextStream("master", 9998, StorageLevel.MEMORY_AND_DISK_SER_2)
  //设定初始值
    val initialRDD = sc.parallelize(Seq(("boy", 100L), ("girl", 50L)))

    val words = lines.flatMap(_.split(" "))
    val wordsPairs = words.map((_, 1))
    /**
      * currentBatchTime：当前时间
      * key： key
      * values：对应key前1秒收到的结果集
      * currentState: 对应key的当前状态
      */
    val stateSpec = StateSpec.function((currentBatchTime: Time, key:String, values: Option[Int], currentState: State[Long]) => {
      val sum = values.getOrElse(0).toLong + currentState.getOption().getOrElse(0L)
      val output = (key, sum)
      if(!currentState.isTimingOut()){
        currentState.update(sum)
      }

      Some(output)
      //设置初始值为initialRDD， timeout：一个key多长时间没接收到数据时清理状态
    }).initialState(initialRDD).numPartitions(2).timeout(Seconds(30))
        val wordsCount = wordsPairs.mapWithState(stateSpec)
        wordsCount.print()
    //wordCount.stateSnapshots().print() //状态没有更新的key也可以打印出来



    //启动streaming应用
    ssc.start()
    //等待程序运行结束
    ssc.awaitTermination()

    ssc.stop(false)//参数为是否关闭sparkcontext， 默认为关闭
  }
}
