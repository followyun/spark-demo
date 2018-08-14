package com.my.spark.rdd

import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}

/**
  * 分区器的测试
  */
object PartitionerTest {
  def main(args: Array[String]): Unit = {

  }

  /**
    * 测试范围分区器
    */
  def test1(): Unit ={
    val conf = new SparkConf().setAppName("PartitionerTest")
    val sc = new SparkContext(conf)

    val pairRDD = sc.parallelize[(String, Int)](Seq((("hello",1)), ("world", 1),
      ("word", 1), ("count", 1), ("count", 1), ("word", 1), ("as", 1),
      ("example", 1), ("hello", 1), ("word", 1), ("count", 1),
      ("hello", 1), ("word", 1), ("count", 1)), 5) //数字5表示划分为5个分区
    val rangPRDD = pairRDD.partitionBy(new RangePartitioner[String, Int](2, pairRDD))
    rangPRDD.glom().collect()
  }
}
