package com.my.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 测试count api
  */
object CountApiTest {
  val conf = new SparkConf().setAppName("PartitionerTest")
  val sc = new SparkContext(conf)
  def main(args: Array[String]): Unit = {

  }

  /**
    * 测试单类型RDD
    */
  def test1(): Unit ={
    val paralRDD = sc.parallelize(1 to 10000, 20)
    val paralsRDD = paralRDD ++ paralRDD ++ paralRDD ++ paralRDD //连接操作
    paralsRDD.count()
    /*估算
      第一个参数100为超时时间
      第二个参数为期望达到近似估计的准确度
      如果countApprox在超时时间内计算完则不会近似估值
    */
    val countResult = paralsRDD.countApprox(100, 0.9)
    //估计结果平均值
    countResult.initialValue.mean
    //估计结果最高值
    countResult.initialValue.high
    //估计结果最低值
    countResult.initialValue.low
    //估计结果正确度
    countResult.initialValue.confidence
    countResult.getFinalValue().mean

    val countValueResult= paralsRDD.countByValueApprox(1000, 0.9);
    //统计个数，排除重复的value  参数为精确度，值越小结果越精确
    val countDisRes = paralsRDD.countApproxDistinct()//9760
    paralsRDD.countApproxDistinct(0.01)//9947
    paralsRDD.countApproxDistinct(0.005)//9972
  }

  /**
    *测试key-value类型RDD
    */
  def test2(): Unit ={
    val pairRDD = sc.parallelize((1 to 10000).zipWithIndex)
    val pairsRDD = pairRDD ++ pairRDD ++ pairRDD ++ pairRDD ++ pairRDD ++ pairRDD
    val countRes = pairsRDD.countByKeyApprox(100, 0.7)
    //查看key为100的value有多少个
    pairsRDD.lookup(100) //6
    //将数据集转换为map
    pairsRDD.collectAsMap()


  }
}
