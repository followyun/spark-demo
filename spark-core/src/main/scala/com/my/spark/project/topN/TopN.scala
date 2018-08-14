package com.my.spark.project.topN

import java.net.URL

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * 统计前n的数据
  * 我这种做法可能造成内存溢出
  */
object TopN {
  val conf = new SparkConf().setAppName("TopN");
  conf.setMaster("local[*]")
  val sc = SparkContext.getOrCreate(conf)
  val topNDataPath = s"F:\\BigData\\workspace\\spark-demo\\spark-core\\src\\main\\resources\\topNData"


  def main(args: Array[String]): Unit = {
    val wordPath = s"${topNDataPath}/topn/1000000";
    val urlPath = s"${topNDataPath}/url/1000000";
    //getTopN2(wordPath, s"${topNDataPath}/result/top10WithPriorityQueue/", 10, line=>line.split("\\s+").map((_, 1L)))
    getTopN3(urlPath, s"${topNDataPath}/result/top10WithApi/", 10, line=>Iterator.single(new URL(line).getHost).map((_, 1L)))

  }

  /**
    * 可能造成内存溢出
    */
  private def getTopN0(inputPath:String ,outputPath:String, topN:Int, parseFunc: String => TraversableOnce[(String, Long)]): Unit ={
    val wordTextFileRDD = sc.textFile(inputPath)
    val wordMapRDD = wordTextFileRDD.filter(!_.isEmpty).flatMap(parseFunc)
    val wordCountRDD = wordMapRDD.reduceByKey(_+_).sortBy(_._2, false)
    val wordCountArray = wordCountRDD.collect();//数据存放在driver端，可能造成内存溢出
    val arrayBuffer = ArrayBuffer[(String, Long)]()
    //取前10个
    for (i <- 0 to topN){
      arrayBuffer += wordCountArray(i)
    }
    println(arrayBuffer.toList)
    val topTenRDD = sc.parallelize(arrayBuffer.toList, 1)
    topTenRDD.saveAsTextFile(outputPath)
  }


  /**
    * 标准的取topN的方式
    * @param inputPath 输入目录
    * @param outputPath 输出目录
    * @param topN 取得个数
    * @param parseFunc 解析一行数据的函数
    */
  private def getTopN1(inputPath:String ,outputPath:String, topN:Int, parseFunc: String => TraversableOnce[(String, Long)]): Unit = {
    val wordTextFileRDD = sc.textFile(inputPath)
    val wordCountRDD = wordTextFileRDD.filter(!_.isEmpty).flatMap(parseFunc).reduceByKey(_+_)
    val partitionTopN = (buffer: ListBuffer[(String, Long)], currentWordCount:(String, Long))=>{
      buffer += currentWordCount;
      if(buffer.size > topN){
       val sortedBuffer = buffer.sortBy(_._2);
        sortedBuffer.remove(0)
        sortedBuffer
      }else{
        buffer
      }
    }

   val partitionTopNRDD = wordCountRDD.mapPartitions(iter => {
     iter.foldLeft(ListBuffer[(String, Long)]())(partitionTopN).toIterator
    }).repartition(1)

    val topNRDD = partitionTopNRDD.mapPartitions(iter=>{
      iter.foldLeft(ListBuffer[(String, Long)]())(partitionTopN)
        .sortBy(_._2).reverse.toIterator
    })

    FileSystem.get(sc.hadoopConfiguration).delete(new Path(outputPath), true)//删除已存在的数据
    topNRDD.saveAsTextFile(outputPath)
  }

  /**
    * 标准的取topN的方式，使用PriorityQueue（优先级队列）来存放元素
    * @param inputPath 输入目录
    * @param outputPath 输出目录
    * @param topN 取得个数
    * @param parseFunc 解析一行数据的函数
    */
  private def getTopN2(inputPath:String ,outputPath:String, topN:Int, parseFunc: String => TraversableOnce[(String, Long)]): Unit = {
    val wordTextFileRDD = sc.textFile(inputPath)
    val wordCountRDD = wordTextFileRDD.filter(!_.isEmpty).flatMap(parseFunc).reduceByKey(_+_)
    val partitionTopN = ( queue:mutable.PriorityQueue[(String, Long)], currentWordCount:(String, Long))=>{
      queue += currentWordCount;
      if(queue.size > topN){
        queue.dequeue()
      }
      queue
    }

    val ord = new Ordering[(String, Long)]{
      override def compare(x: (String, Long), y: (String, Long)): Int = y._2.compare(x._2)
    }
    val partitionTopNRDD = wordCountRDD.mapPartitions(iter => {
      iter.foldLeft(mutable.PriorityQueue.newBuilder[(String, Long)](ord))(partitionTopN).toIterator
    }).repartition(1)

    val topNRDD = partitionTopNRDD.mapPartitions(iter=>{
      iter.foldLeft(mutable.PriorityQueue.newBuilder[(String, Long)](ord))(partitionTopN).toIterator
    })

    FileSystem.get(sc.hadoopConfiguration).delete(new Path(outputPath), true)//删除已存在的数据
    topNRDD.saveAsTextFile(outputPath)
  }

  /**
    * 标准的取topN的方式，使用api  top()来进行操作
    * @param inputPath 输入目录
    * @param outputPath 输出目录
    * @param topN 取得个数
    * @param parseFunc 解析一行数据的函数
    */
  private def getTopN3(inputPath:String ,outputPath:String, topN:Int, parseFunc: String => TraversableOnce[(String, Long)]): Unit = {
    val wordTextFileRDD = sc.textFile(inputPath)
    val wordCountRDD = wordTextFileRDD.filter(!_.isEmpty).flatMap(parseFunc).reduceByKey(_+_)

   val topNArr =  wordCountRDD.top(topN)(ord = new Ordering[(String, Long)](){
      override def compare(x: (String, Long), y: (String, Long)): Int = x._2.compare(y._2)
    })

    FileSystem.get(sc.hadoopConfiguration).delete(new Path(outputPath), true)//删除已存在的数据
    sc.parallelize(topNArr, 1).saveAsTextFile(outputPath)
  }
}
