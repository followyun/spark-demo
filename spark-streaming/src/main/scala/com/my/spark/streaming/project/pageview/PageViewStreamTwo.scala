package com.my.spark.streaming.project.pageview

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * kafka+flume+streaming+redis对网站用户访问情况进行实时分析
  * flume收集日志文件数据
  * kafka作为flume的一个channel
  * streaming从kafka取数据并进行分析
  * 分析结果发往redis
  */

/**
  * 操作步骤
  *1. 启动redis
  * src/redis-server &
  * 如果没有设置密码的话，则打开src/redis-cli。然后执行 CONFIG SET protected-mode no
  *
  * 2. 准备数据到webserver.log文件中
  * echo http://www.spark-learn.com 100 89978 3232 >> webserver.log
  *
  * 3. 启动kafka
  * nohup bin/kafka-server-start.sh config/server.properties >~/bigdata/kafka_2.11-1.0.0/logs/server.log 2>&1 &
  * 创建topic pageview
  * bin/kafka-topics.sh --create --zookeeper master:2181 --replication-factor 3 --partitions 1 --topic pageview
  * 4. 启动flume
  * bin/flume-ng agent --conf conf --conf-file conf/flume-conf_pageview.properties --name agent1
  *5. 提交streaming应用
  * spark-submit \
  * --class com.my.spark.streaming.project.pageview.PageViewStreamTwo \
  * --master spark://master:7077 \
  * --deploy-mode client \
  * --driver-memory 512m \
  * --executor-memory 512m \
  * --total-executor-cores 4 \
  * --executor-cores 2 \
  * ~/mr-course/spark-streaming-1.0-SNAPSHOT-jar-with-dependencies.jar \
  * master:9092,slave1:9092,slave2:9092 pageview
  */
object PageViewStreamTwo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PageViewStreamTwo")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))
    val Array(brokerList, topics) = args
    ssc.checkpoint("hdfs://master:9999/user/bd-cqr/streaming/checkpoint")
    val kafkaParam = Map[String, String]("metadata.broker.list" -> brokerList)

    val topicSet = topics.split(",").toSet
    val receiveDS = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, topicSet)
    //每隔5秒统计前30秒用户访问量
    val pageViewDS = receiveDS.map(pvStr => {
      val parts = pvStr._2.split(" ")
      new PageView(parts(0), parts(1).toInt, parts(2).toInt, parts(3).toInt)
    })
    //    val activeUserCount = pageViewDS.window(Seconds(30), Seconds(5)).map(pv => (pv.userID, 1)).groupByKey().count()
    //每隔3秒统计前50秒每一个url的访问错误数
    val errorAccessCount = pageViewDS.window(Seconds(50), Seconds(3)).map(pv => {
      println("------pv = " + pv)
      val normalCode = 200
      var errorCount = 0
      if (pv.status != normalCode)
        errorCount = 1
      (pv.url, errorCount)
    }).reduceByKey(_ + _)

    errorAccessCount.print()

    //每隔2秒统计前15秒有多少个用户访问了url: http://foo.com/
    val theUrlAccessCount = pageViewDS.window(Seconds(15), Seconds(2)).filter(pv => {
      val theUrl = "http://foo.com/"
      pv.url.equals(theUrl)
    }).map(pv => (pv.userID, 1)).groupByKey().count()

    theUrlAccessCount.print()

    //保存统计结果到redis
    //    activeUserCount.foreachRDD {
    //      rdd =>
    //        rdd.foreachPartition {
    //          partitionRecords =>
    //            val jedis = RedisClient.getPool.getResource
    //            partitionRecords.foreach {
    //              count => jedis.set("active_user_count", count.toString)
    //            }
    //            RedisClient.getPool.returnResource(jedis)
    //        }
    //    }

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}

