package com.my.spark.streaming.recevier

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 自定义接收器的使用
  */
object CustomStream {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CustomStream")
    val sc = new SparkContext(conf)

    //创建spark streaming编程入口，处理间隔为1S
    val ssc = new StreamingContext(sc, Seconds(1))
    //创建一个自定义接收器，接收master服务器发送过来的数据
    val lines = ssc.receiverStream(new CustomRecevier("master", 9998))

    val words = lines.flatMap(_.split(" "))
    val wordsPairs = words.map((_, 1))
    val wordsCount = wordsPairs.reduceByKey(_ + _)
    wordsCount.print()
    //启动streaming应用
    ssc.start()

    //等待程序运行结束
    ssc.awaitTermination()
  }
}

class CustomRecevier(host: String, port: Int)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {
  override def onStart(): Unit = {
    //启动一个线程接收数据
    new Thread("Socket Recevier") {
      override def run() {
        receive()
      }
    }.start()


  }

  override def onStop(): Unit = {}

  private def receive() {
    var socket: Socket = null
    var userInput: String = null
    try {
      logInfo("connecting to " + host + ":" + port)
      socket = new Socket(host, port)
      logInfo("connected to " + host + ":" + port)
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))
      userInput = reader.readLine()
      if (userInput != null && !isStopped()) {
        store(userInput)
        userInput = reader.readLine()
      }

      reader.close()
      socket.close()
      logInfo("Stopped Receiving")
      restart("Try to connect Again")
    }catch {
      case e: java.net.ConnectException =>
        restart("Error connecting to " + host + ":" + port, e)
      case t: Throwable =>
        restart("Error receiving data", t)
    }
  }
}
