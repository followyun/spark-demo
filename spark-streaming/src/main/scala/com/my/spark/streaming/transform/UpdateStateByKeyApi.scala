package com.my.spark.streaming.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
  */
object UpdateStateByKeyApi {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("NetWorkWordCount")
    val sc = new SparkContext(conf)

    //创建spark streaming编程入口，处理间隔为1S
    val ssc = new StreamingContext(sc, Seconds(1))
    //必须先设置好checkpoint后才能使用updatStateByKey
    ssc.checkpoint("hdfs://master:9999/user/bd-cqr/streaming/checkpoint")
    //创建一个接收器，接收master服务器发送过来的数据

    val lines = ssc.socketTextStream("master", 9998, StorageLevel.MEMORY_AND_DISK_SER_2)

    val words = lines.flatMap(_.split(" "))
    val wordsPairs = words.map((_, 1))
    //values 为前1秒收集到的数据集合，currentState为之前累积的计算结果
//    val wordsCount = wordsPairs.updateStateByKey((values:Seq[Int], currentState:Option[Int])=> Some(currentState.getOrElse(0) + values.sum)
//    )
//    wordsCount.print()

    //只统计包含good的单词
    //updateStateByKey的另一个API 可以对key值进行筛选， iter元素类型String为key， Seq[Int]为数据集合，Option[Int]为上一次计算的结果
    val wordsCount1 = wordsPairs.updateStateByKey[Int]((iter:Iterator[(String, Seq[Int], Option[Int])]) =>{
      val list = ListBuffer[(String, Int)]()
      while (iter.hasNext){
        val (key, newCounts, currentState) = iter.next()
        if(key.contains("good")){
          list += ((key, currentState.getOrElse(0) + newCounts.sum))
        }
      }
      list.toIterator
    }, new HashPartitioner(4), true //true表示生成新的DStream用hashpartitioner进行重新分区
    )
    wordsCount1.print()

    //启动streaming应用
    ssc.start()
    //等待程序运行结束
    ssc.awaitTermination()

    ssc.stop(false)//参数为是否关闭sparkcontext， 默认为关闭
  }
}
