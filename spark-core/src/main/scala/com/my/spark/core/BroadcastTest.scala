package com.my.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 广播机制
  * 当每个executor端都需要driver端的指定数据时
  */
object BroadcastTest {
  val conf = new SparkConf().setAppName("BinaryRDDApiTest")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {

  }

  def test1(): Unit = {
    val map = Map(1 -> "one", 2 -> "two")
    val broadMap = sc.broadcast(map)//将RDD广播到每一个excutor
    val paralRDD = sc.parallelize(Seq(1, 2, 3, 4, 5), 2)
    paralRDD.foreach(i=>{
      val value = broadMap.value.get(i)
      if(value.isDefined){
        println(s"${value} is define : ${value.get}")
      }
    })

  }

}
