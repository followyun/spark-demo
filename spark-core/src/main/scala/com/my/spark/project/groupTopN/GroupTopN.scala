package com.my.spark.project.groupTopN

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * 分组求topN
  */
object GroupTopN {
  val conf = new SparkConf().setAppName("GroupTopN")
  conf.setMaster("local[*]") // *表示用本机的所有cpu
  val sc = SparkContext.getOrCreate(conf)

  def main(args: Array[String]): Unit = {
    groupTopN1

  }

  /**
    * 使用了groupByKey
    * 可能造成数据倾斜
    * 可能造成oom
    */
  def groupTopN0(): Unit = {
    val dataPath = "F:\\BigData\\workspace\\spark-demo\\spark-core\\src\\main\\resources\\"
    val textFileRDD = sc.textFile(s"${dataPath}/topNData/groupn/")
    val keyValueRDD = textFileRDD.map(line => {
      val fields = line.split(",")
      (fields(0), fields(1).toInt)
    })
    val groupTopNRDD = keyValueRDD.groupByKey().map(iters => {
      (iters._1, iters._2.toList.sorted.take(3)) //可能造成内存溢出，或key的数据较多的executor端运行速度慢
    })

    //    val groupTopNRDD = keyValueRDD.groupByKey().map{
    //      case (key, iters)=>(key, iters.toList.sorted(Ordering.Int.reverse).take(3))
    //    }

    groupTopNRDD.saveAsTextFile(s"${dataPath}/topNData/result/groupTopN/")
  }

  /**
    * 通过两次聚合操作来解决oom和数据倾斜问题
    * 原理是将key再划分为多个不同的key，分散分组后key对应的数据量
    */
  def groupTopN1(): Unit ={
    val dataPath = "F:\\BigData\\workspace\\spark-demo\\spark-core\\src\\main\\resources\\"
    val textFileRDD = sc.textFile(s"${dataPath}/topNData/groupn/")
    def randomNum = ()=>{Random.nextInt(10)}
    val firstkeyValueRDD = textFileRDD.map(line => {
      val fields = line.split(",")
      (randomNum() + "-" + fields(0), fields(1).toInt)
    })
  val orderValue = (iter:TraversableOnce[Int], topN: Int)=>{
    iter.toList.sorted(Ordering.Int.reverse).take(topN)
  }
    val secondKVRDD = firstkeyValueRDD.groupByKey().flatMap{
      case (key, iter)=>
        orderValue(iter, 3).map((key.split("-")(1), _))

    }

    val groupTopNRDD = secondKVRDD.groupByKey().mapValues(
      iter=>orderValue(iter, 3)
    )


    groupTopNRDD.saveAsTextFile(s"${dataPath}/topNData/result/groupTopN/")
  }
}
