package com.my.spark.core

import java.util.concurrent.ConcurrentHashMap
import java.util.function.BiConsumer

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 累加器测试
  */
object AccumulatorTest {
  val conf = new SparkConf().setAppName("AccumulatorTest")
  val sc = new SparkContext(conf)
  def main(args: Array[String]): Unit = {

  }

  def test1(): Unit ={
   val paralRDD = sc.parallelize(Seq("plane", "fish", "duck", "dirty", "people", "plane"))
    val count = sc.longAccumulator("统计paralRDD中单词个数")
    val containt = sc.collectionAccumulator[String]("存放单词的容器")

    val mycount = new MyAccumulator
    sc.register(mycount) //注册自定义accumulator

    paralRDD.foreach(v => {
      count.add(1)
      containt.add(v)
      mycount.add(v)
    })

    println(count.value) //6
    println(containt.value) //[fish, duck, people, plane, dirty, plane]
    println(mycount.value) //{plane=2, dirty=1, duck=1, fish=1, people=1}
  }


}
/**
  * 自定义累加器，用于统计每个单词的个数
  */
class MyAccumulator extends AccumulatorV2[String, ConcurrentHashMap[String, Int]]{

  private val map = new ConcurrentHashMap[String, Int]()
  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[String, ConcurrentHashMap[String, Int]] ={
    val newAcc = new MyAccumulator()
    newAcc.map.putAll(map)
    newAcc
  }

  override def reset(): Unit = map.clear()

  /**
    * 在task端执行
    * @param v
    */
  override def add(v: String): Unit = {
    map.synchronized{
      if(map.containsKey(v)){
        map.put(v, map.get(v)+1)
      }else{
        map.put(v, 1)
      }
    }
  }

  /**
    * 在driver端，即客户端执行
    * @param other
    */
  override def merge(other: AccumulatorV2[String, ConcurrentHashMap[String, Int]]): Unit = other match {
    case o:MyAccumulator =>{
      o.map.forEach(new BiConsumer[String, Int] {
        override def accept(key: String, value: Int): Unit = {
          if (map.containsKey(key)) {
            map.put(key, map.get(key) + value)
          } else {
            map.put(key, value)
          }
        }
      })
    }
  }

  override def value: ConcurrentHashMap[String, Int] = map
}
