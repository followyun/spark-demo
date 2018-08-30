package com.my.spark.streaming.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Streaming 与kafka的集成(Receiver模式)，统计单词个数
spark-submit \
--class com.my.spark.streaming.kafka.ReceiveKafkaDataAtReceiverMode \
--master spark://master:7077 \
--deploy-mode client \
--driver-memory 512m \
--executor-memory 512m \
--total-executor-cores 4 \
--executor-cores 2 \
~/mr-course/spark-streaming-1.0-SNAPSHOT-jar-with-dependencies.jar \
master,slave1,slave2 my-consumer-group topic1,topic2 1
  */
object ReceiveKafkaDataAtReceiverMode {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ReceiveKafkaData")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))

    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, numThreads) = args

    ssc.checkpoint("hdfs://master:9999/user/bd-cqr/streaming/checkpoint")
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val kafkaParms = Map[String, String] (
      "zookeeper.connect" -> zkQuorum
      ,
      "zookeeper.connection.timeout.ms" -> s"10000"
      ,
      "group.id" -> group
      ,
      ".auto.offer.reset" -> s"largest"
    )
      val kafkaDStream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParms, topicMap, StorageLevel.MEMORY_AND_DISK_SER_2)
    kafkaDStream.map(x => println(x._1))
    //创建多个receiver
    val receiverNums = 3
    val kafkaDStreams = (1 to receiverNums).map{
      _=>
        KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
          ssc, kafkaParms, topicMap, StorageLevel.MEMORY_AND_DISK_SER_2
        )
    }
    //使用ssc.union方法合并多个DStream
    val unionDStream = ssc.union(kafkaDStreams)
    val lines = kafkaDStream.map(_._2)
    val wordCounts = lines.flatMap(_.split(",")).map((_, 1)).reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(false)
    }
}
