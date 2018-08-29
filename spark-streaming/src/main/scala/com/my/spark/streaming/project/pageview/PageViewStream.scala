package com.my.spark.streaming.project.pageview

import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._

/**
  * 网站流量实时监控
  * 数据格式为 url\tstatus\tzipCode\tuserID\n
  */
object PageViewStream {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      System.err.println("Usage: PageViewStream <metric> <host> <port>")
      System.err.println("<metric> must be one of pageCounts, slidingPageCounts," +
        " errorRatePerZipCode, activeUserCount, popularUsersSeen")
      System.exit(1)
    }

    val metric = args(0)
    val host = args(1)
    val port = args(2)

    //创建streaming上下文
    val ssc = new StreamingContext("local[*]", "PageViewStream", Seconds(1),
      System.getenv("SPARK_HOME"),SparkContext.jarOfClass(this.getClass).toSeq
    )
    ssc.checkpoint("/tmp/checkpoint")

    //创建一个socket接收器，接收 host:port发来的数据
    val pageViewDS = ssc.socketTextStream(host, port.toInt).flatMap(_.split("\n"))
      .map(PageView.fromString _)

    //1. 统计每1秒url被访问的次数
    val urlCountDS = pageViewDS.map(pv => (pv.url, 1)).reduceByKey(_ + _)
//    val urlCountDS = pageViewDS.map(_.url).countByValue() // 等同于上一句
   // urlCountDS.print()

    //2. 每隔2秒统计前10秒内每一个Url被访问的次数
    val urlCountWindowDS = pageViewDS.map(_.url).countByValueAndWindow(Seconds(10), Seconds(2))
//    urlCountWindowDS.print()

    //3. 每隔2秒统计前30秒内每一个地区邮政编码的访问错误率（status非200的，表示是访问错误页面）
    val statusesPerZipCode = pageViewDS.window(Seconds(30), Seconds(2)).map(pv => (pv.zipCode, pv.status)).groupByKey()
    val errorStatusPerZipCode = statusesPerZipCode.map{
      case (zipCode, values) =>
        var errorCodeCount = 0
        val totalCount = values.size
        values.foreach(status => if(status != 200) errorCodeCount += 1) // ==errorCodeCount = values.count(_ != 200)
        (zipCode, errorCodeCount.toDouble / totalCount.toDouble)
    }
//    errorStatusPerZipCode.print()

    //4. 每隔2秒统计前15秒内有多少个活跃用户
    //外部数据源，用于与流数据进行关联
    val userList = ssc.sparkContext.parallelize(Seq(1->"bd-cqr",
      2->"jeffy", 3->"katy"))
    val activeUserCountDS = pageViewDS.window(Seconds(15), Seconds(2)).map(pv => (pv.userID ,1)).
      groupByKey().count()
//    activeUserCountDS.print()

    //通过args(0)使用不同的流处理
    metric match {
      case "pageCounts" => urlCountDS.print()
      case "slidingPageCounts" => urlCountWindowDS.print()
      case "errorRatePerZipCode" => errorStatusPerZipCode.print()
      case "activeUserCount" => activeUserCountDS.print()
      // 5、统计每隔1秒内在已经存在的user是否为活跃用户
      case "popularUsersSeen" =>
        pageViewDS.map(pv => (pv.userID, 1)).foreachRDD(
          (rdd, time)=>{
            rdd.join(userList).map(_._2._2).take(10).foreach(u => println("Saw user %s at time %s".format(u, time)))
          }
        )
      case _ =>println("error metric" + metric)
    }

    ssc.start()

    ssc.awaitTermination()
  }
}
