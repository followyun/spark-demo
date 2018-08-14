package com.my.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  */
object RDDCreateTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDDCreateTest")
    val sc = new SparkContext(conf)
    //创建RDD的方法
    //1. 从文件系统，如hdfs
    val hdfsRDD = sc.textFile("hdfs://master:9999/user/bd-cqr/word.txt")
    hdfsRDD.count()

    //2. 通过已存在的RDD来创建
    val mapRDD = hdfsRDD.map(_+"test")
    mapRDD.count()

    //3. 通过内存已存在的序列化列表来创建，可以指定分区，如果不指定的话分区数为所有executor的cores数
    val listRDD = sc.parallelize(Seq(1,2,3,4,5), 2)//分为2个区
    listRDD.partitions//获取分区
    listRDD.preferredLocations(listRDD.partitions(0)) //获取某个分区数据位于哪个主机上
    listRDD.dependencies//获取父RDD
    //listRDD.compute() //计算
    listRDD.collect()
    listRDD.glom().collect()//查看每个分区上的数据

    val rangeRDD = sc.range(0, 10, 2, 3)//创建0， 10的Range， 步长为2， 分为3个区

    val makeRDD = sc.makeRDD(Seq(1,2,3,4,5), 2) // == sc.parallelize(Seq(1,2,3,4,5), 2)
    //创建指定在数据在某个分区的RDD
    val specialLocationMakeRDD = sc.makeRDD(Seq((Seq(1,2), Seq("192.168.112.131")), (Seq(3,4,5), Seq("192.168.112.131"))))
    specialLocationMakeRDD.glom().collect()
  }
}
