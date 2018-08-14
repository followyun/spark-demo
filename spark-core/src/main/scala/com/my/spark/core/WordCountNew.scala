package com.my.spark.core

import com.my.spark.util.JavaHDFSUtil
import org.apache.hadoop.io.{LongWritable, _}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * 使用上RDD类型后的单词统计
  */
object WordCountNew {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf()
    conf.setAppName("wordCount")

    val sc = new SparkContext(conf)

    val inputRdd: RDD[(LongWritable, Text)] = sc.hadoopFile("hdfs://master:9999/user/bd-cqr/word.txt", classOf[TextInputFormat], classOf[LongWritable], classOf[Text])
    inputRdd.partitions.size //查看有几个分区
    inputRdd.partitioner //查看分区器
    val words: RDD[String] = inputRdd.flatMap(_._2.toString.split(" "))
    inputRdd.dependencies //查看依赖
    inputRdd.dependencies.map(_.rdd) //查看具体依赖类型
    val wordCount: RDD[(String, Int)] = words.map((_, 1))
    val counts: RDD[(String, Int)] = wordCount.reduceByKey(new HashPartitioner(2), _ + _)
    val destPath = "hdfs://master:9999/user/bd-cqr/wordcount"
    JavaHDFSUtil.deleteFileIfExisted(destPath)
    counts.saveAsTextFile(destPath)//保存到磁盘中
    counts.collect//获取结果集
    counts.persist()//加入缓存
    counts.unpersist()//取消缓存
    sc.stop()
  }
}
