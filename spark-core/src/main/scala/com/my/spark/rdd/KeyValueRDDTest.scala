package com.my.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * 测试Key-Value RDD
  */
object KeyValueRDDTest {
  val conf = new SparkConf().setAppName("PartitionerTest")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    test1
  }

  /**
    * combineByKey
    * combineByKeyWithClassTag
    */
  def test1(): Unit = {
    val pairStrRDD = sc.parallelize[(String, Int)](Seq(("coffee", 1),
      ("coffee", 2), ("panda", 3), ("coffee", 9)), 2)

    def createCombiner = (value: Int) => (value, 1)

    def mergeValue = (acc: (Int, Int), value: Int) => (acc._1 + value, acc._2 + 1)

    def mergeCombiners = (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc2._2 + acc1._2)

    val combineByKeyRDD = pairStrRDD.combineByKey(createCombiner, mergeValue, mergeCombiners)
    combineByKeyRDD.collect()
  }

  /**
    * 以下几个api都是基于combineByKey实现的
    * aggregateByKey()
    * reduceByKey()
    * foldByKey()
    * groupByKey()通过key进行分组
    */
  def test2(): Unit = {
    val pairRDD = sc.parallelize[(Int, Int)](Seq((1, 2), (3, 4), (3, 6), (5, 6)), 2)

    def mergeValueAggregate = (acc: (Int, Int), value: Int) => (acc._1 + value, acc._2 + 1)

    def mergeCombinersAggregate = (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc2._2 + acc1._2)

    val aggregateRDD = pairRDD.aggregateByKey((0, 0))( //createCombiner = ((0,0),v)
      mergeValueAggregate, //==mergeValue
      mergeCombinersAggregate) // ==mergeCombiners
    aggregateRDD.collect() // Array((1,(2,1)), (3,(10,2)), (5,(6,1)))

    val reduceRDD = pairRDD.reduceByKey((x, y) => x + y) // == pairRDD.combineByKey(v=>v, (x:Int, y:Int) => x + y, (x:Int, y:Int)=>x + y)
    reduceRDD.collect() // Array((1,2), (3,10), (5,6))
    //返回结果为map
    pairRDD.reduceByKeyLocally((x, y) => x + y)  // Map(1 -> 2, 3 -> 10, 5 -> 6)

    val foldRDD = pairRDD.foldByKey(0)((x, y) => x + y) //pairRDD.combineByKey(v=>v, (x:Int, y:Int) => x + y, (x:Int, y:Int)=>x + y)
    foldRDD.collect() //Array((1,2), (3,10), (5,6))

    val groupRDD = pairRDD.groupByKey() // == pairRDD.combineByKey[List[Int]]((v:Int)=>List(v), (arr:List[Int], v:Int)=> (v::arr), (arr1:List[Int], arr2:List[Int])=>(arr1 ::: arr2))

    groupRDD.collect() // Array((1,CompactBuffer(2)), (3,CompactBuffer(4, 6)), (5,CompactBuffer(6)))
  }

  /**
    * cogroup api
    */
  def test3(): Unit ={
    val pairRDD = sc.parallelize[(Int, Int)](Seq((1, 2), (3, 4), (3, 6), (5, 6)), 4)
    val otherRDD = sc.parallelize(Seq((3,9), (4, 5)))

    pairRDD.cogroup(otherRDD).collect()//Array((4,(CompactBuffer(),CompactBuffer(5))),(1,(CompactBuffer(2),CompactBuffer())), (5,(CompactBuffer(6),CompactBuffer())), (3,(CompactBuffer(6, 4),CompactBuffer(9))))

    //筛选两个RDD中具有的相同key的数据，不同则不会加入
    pairRDD.join(otherRDD).collect //Array((3,(9, 4)), (3, (6, 9)))
    val otherRDD1 = sc.parallelize(Seq((3,9), (4, 5), (3, 8)))
    pairRDD.join(otherRDD1).collect //Array((3,(4,9)), (3,(4,8)), (3,(6,9)), (3,(6,8)))

    //包含左边RDD所有key，包含右边与左边有相同的key的数据
    pairRDD.leftOuterJoin(otherRDD).collect() //Array((1,(2,None)), (5,(6,None)), (3,(4,Some(9))), (3,(6,Some(9))))

    //包含右边RDD所有key，包含左边与右边有相同的key的数据
    pairRDD.rightOuterJoin(otherRDD).collect() // Array((4,(None,5)), (3,(Some(4),9)), (3,(Some(6),9)))

    //包含左右两边RDD所有的key
    pairRDD.fullOuterJoin(otherRDD).collect() //Array((4,(None,Some(5))), (1,(Some(2),None)), (5,(Some(6),None)), (3,(Some(4),Some(9))), (3,(Some(6),Some(9))))

    //筛选出与otherRDD中key不同的数据
    pairRDD.subtractByKey(otherRDD).collect() // Array((1,2), (5,6))
  }

  /**
    * 其它api
    * flatMapValues()
    * mapValues()
    * sortBy()
    * sortByKey()
    * filterByRange()
    *
    */
  def test4(): Unit ={
    val pairRDD = sc.parallelize[(Int, Int)](Seq((1, 2), (3, 4), (3, 6), (5, 6)), 4)
    pairRDD.sortByKey()
  }
}
