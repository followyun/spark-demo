package com.my.submit.scheduler

import java.util.concurrent.CyclicBarrier

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 公平调度器测试（公平调度TaskManager）
  */
object FairAppScheduler {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FairAppScheduler")
    conf.setMaster("local[*]")
    conf.set("spark.scheduler.mode", "FAIR")//设置调度器模式为公平调度
    conf.set("spark.scheduler.allocation.file", "F:\\BigData\\workspace\\spark-demo\\spark-submit\\src\\main\\resources\\fairscheduler.xml")   //获取配置文件

    val sc = new SparkContext(conf)

    var friendCount = 0L
    var orderCount = 0L

    val barrier = new CyclicBarrier(2, new Runnable {
      override def run(): Unit = {
        println("start save count...")
        //ThreadLocal级别
        sc.setLocalProperty("spark.scheduler.pool", null) //使用defaultPool
        val total = friendCount + orderCount
        val totalRDD = sc.parallelize(Seq(total), 1)
        totalRDD.saveAsTextFile("file:///F:\\BigData\\workspace\\spark-demo\\spark-submit\\src\\main\\resources\\fairappscheduler\\")
      }
    })

    new Thread(){
      override def run(): Unit = {
        println("start count friend.txt num")
        sc.setLocalProperty("spark.scheduler.pool", "Pool1")
        friendCount = sc.textFile("file:///F:\\BigData\\workspace\\spark-demo\\spark-submit\\src\\main\\resources\\friend.txt").count()
        barrier.await()
      }
    }.start()

    new Thread(){
      override def run(): Unit = {
        println("start count order.txt num")
        sc.setLocalProperty("spark.scheduler.pool", "Pool2")
        orderCount = sc.textFile("file:///F:\\BigData\\workspace\\spark-demo\\spark-submit\\src\\main\\resources\\order.txt").count()
        barrier.await()
      }
    }.start()
  }
}
