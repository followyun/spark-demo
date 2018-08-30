package com.my.spark.streaming.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Streaming 与kafka的集成(Direct模式)，统计单词个数
spark-submit \
--class com.my.spark.streaming.kafka.ReceiveKafkaDataAtDirectMode \
--master spark://master:7077 \
--deploy-mode client \
--driver-memory 512m \
--executor-memory 512m \
--total-executor-cores 4 \
--executor-cores 2 \
~/mr-course/spark-streaming-1.0-SNAPSHOT-jar-with-dependencies.jar \
master:9092,slave1:9092,slave2:9092 topic1,topic2
  */
object ReceiveKafkaDataAtDirectMode {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ReceiveKafkaDataAtDirectMode")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))

    if (args.length < 2) {
      System.err.println("Usage: KafkaWordCount <broker-list> <topics> ")
      System.exit(1)
    }

    val Array(brokerList, topics) = args

    ssc.checkpoint("hdfs://master:9999/user/bd-cqr/streaming/checkpoint")
    val topicMap = topics.split(",").toSet
    val kafkaParms = Map[String, String] (
      "metadata.broker.list" -> brokerList
    )
      val kafkaDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParms, topicMap)

    val lines = kafkaDStream.map(_._2)
    val wordCounts = lines.flatMap(_.split(",")).map((_, 1)).reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(false)
    }
}
