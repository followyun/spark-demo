package com.my.spark.streaming.recevier

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 实时流处理例子
  * 实时统计socket接收到的单词个数
  * 1. 提交spark straming应用
  * spark-submit \
  * --class com.my.spark.streaming.xxx \
  * --master spark://master:7077 \
  * --deploy-mode client \
  * --driver-memory 512m \
  * --executor-memory 512m \
  * --total-executor-cores 4 \
  * --executor-cores 2 \
  * ~/mr-course/spark-streaming-1.0-SNAPSHOT.jar
  **
  *2. 在spark-shell中使用
  *spark-shell --master spark://master:7077 --total-executor-cores 4 --executor-cores 2
  */
object NetWorkWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("NetWorkWordCount")
    val sc = new SparkContext(conf)

    //创建spark streaming编程入口，处理间隔为1S
    val ssc = new StreamingContext(sc, Seconds(1))
    //创建一个接收器，接收master服务器发送过来的数据
    val lines = ssc.socketTextStream("master", 9998, StorageLevel.MEMORY_AND_DISK_SER_2)

    val words = lines.flatMap(_.split(" "))
    val wordsPairs = words.map((_, 1))
    val wordsCount = wordsPairs.reduceByKey(_+_)
    wordsCount.print()
    //启动streaming应用
    ssc.start()

    //等待程序运行结束
    ssc.awaitTermination()
    //2：StreamingContext的注意事项：
    // 2.1、在同一个时间内，同一个JVM中StreamingContext只能有一个
    // 2.2、如果一个StreamingContext启动起来了，
    //    那么我们就不能为这个StreamingContext添加任何的新的Streaming计算
    // 2.3、如果一个StreamingContext被stop了，那么它不能再次被start
    // 2.4、一个SparkContext可以启动多个StreamingContext，
    //    前提是前面的StreamingContext被stop掉了，而SparkContext没有被stop掉
    //停止spark streaming
    ssc.stop(false)//参数为是否关闭sparkcontext， 默认为关闭
  }
}
