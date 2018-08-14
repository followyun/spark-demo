package com.my.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 二元RDD api测试
  */
object BinaryRDDApiTest {
  val conf = new SparkConf().setAppName("BinaryRDDApiTest")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {

  }

  def test1(): Unit ={
    //instersection() //基于cogroup实现的
    val oneRDD = sc.parallelize(Seq(1,3), 2)
    val twoRDD = sc.parallelize(Seq(3,5, 7), 2)
    val threeRDD = sc.parallelize(Seq((1,2),(3,4), (7, 9)), 2)
    val fourRDD = sc.parallelize(Seq(3,7), 2)
    oneRDD.intersection(twoRDD)//Array(3)
//    oneRDD.intersection(threeRDD) // error 不同类型value的RDD不能进行取交集

    //cartesian  笛卡尔积
    val certesianRDD = oneRDD.cartesian(twoRDD) //Array((1,3), (1,5), (1,7), (3,3), (3,5), (3,7))

    val zipRDD = oneRDD.zip(fourRDD) //
    val zipPartitionRDD = oneRDD.zipPartitions(twoRDD)((iterator1, iterator2)=>Iterator(iterator1.max + iterator2.max))
      zipPartitionRDD.collect // Array(4, 10)
  }
}
