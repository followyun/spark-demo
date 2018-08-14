package com.my.spark.rdd

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  */
object PageRankTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PageRankTest")
    val sc = new SparkContext(conf)

    //创建RDD links， 并使用HashPartitioner分区器进行分区
    val links = sc.parallelize[(String, Seq[String])](Seq(("baidu.com", Seq("jd.com", "ali.com")),
      ("ali.com", Seq("test.com")),
      ("jd.com", Seq("baidu.com")))).partitionBy(new HashPartitioner(3)).persist()
    var ranks = links.mapValues(v=>1.0)
    
    for(i <- 1 until 10){
      val contributions = links.join(ranks).flatMap{
        case (pageId, (link, rank)) => link.map(dest=>(dest, rank / link.size))
      }
      ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v=>0.15 + 0.85 * v)
    }


  }
}
