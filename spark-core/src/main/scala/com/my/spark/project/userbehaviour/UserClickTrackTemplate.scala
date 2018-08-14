package com.my.spark.project.userbehaviour

import java.net.URL
import java.util.UUID

import com.esotericsoftware.kryo.Kryo
import com.my.rdd.{TrackerLog, TrackerSession}
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.{AvroParquetOutputFormat, AvroWriteSupport}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * 网站用户行为分析示例
  */
object UserClickTrackTemplate {
  private val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  private val sessionTimeInterval = 30 * 60 * 1000

  private val logTypeSet = Set("pageview", "click")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UserClickTrackTemplate")

    if (args.size == 0) {
      conf.setMaster("local")
    }

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "com.twq.spark.rdd.example.ClickTrackerKryoRegistrator")

    val sc = new SparkContext(conf)

    val numPartitions = conf.getInt("spark.session.groupBy.numPartitions", 10)

    val trackerDataPath = conf.get("spark.tracker.trackerDataPath",
      "F:\\BigData\\workspace\\spark-demo\\spark-core\\src\\main\\resources\\")
    val domainLabelMap = Map("www.baidu.com" -> "level1", "www.ali.com" -> "level2",
      "jd.com" -> "level3", "youku.com" -> "level4")

    val domainLabelMapB = sc.broadcast(domainLabelMap)

    val sessionCountAccumulator = sc.longAccumulator("session count")

    val rowLogRDD = sc.textFile(s"${trackerDataPath}/visit_log.txt")

    // 对每一条原始日志进行解析成TrackerLog对象，并过滤掉logType不是"pageview", "click"的日志
    val parseLogRDD = rowLogRDD.flatMap(line => parseRowLog(line)).
      filter(log => logTypeSet.contains(log.getLogType.toString)).persist(StorageLevel.DISK_ONLY)
    val partitioner = new HashPartitioner(numPartitions)

    val sessionRDD = parseLogRDD.groupBy((log: TrackerLog) => log.getCookie.toString, partitioner).flatMapValues {
      case iter => {
        //对cookie下的日志按日志的时间进行升序排序
        val sortedParseLogs = iter.toArray.sortBy(_.getLogServerTime.toString)
        val sessionParseLogsResult = cutSession(sortedParseLogs)
        //根据一个会话中的所有日志计算出新的会话对象
        sessionParseLogsResult.map {
          case logs =>
            val session = new TrackerSession()
            session.setCookie(logs(0).getCookie)
            session.setDomain(new URL(logs(0).getUrl.toString).getHost)
            session.setDomainLabel(domainLabelMapB.value.getOrElse(session.getDomain.toString, "-"))
            session.setIp(logs(0).getIp)
            session.setLandingUrl(logs(0).getUrl)
            session.setSessionId(UUID.randomUUID().toString)
            session.setSessionServerTime(logs(0).getLogServerTime)
            session.setClickCount(logs.filter(_.getLogType == "click").size)
            session.setPageviewCount(logs.filter(_.getLogType == "pageview").size)

            sessionCountAccumulator.add(1)

            session

        }
      }
    }

    //获取CookieLabel数据
    val cookieLabelRDD = sc.textFile(s"${trackerDataPath}/cookie_label.txt").map(label => {
      val fields = label.split("\\|")
      (fields(0), fields(1))
    })

    //左连接，针对key-valueRDD，根据key值相同进行组合
    val labeledSessionRDD = sessionRDD.leftOuterJoin(cookieLabelRDD).map {
      case (cookie, (session, cookieLabelOpt)) => cookieLabelOpt.foreach(session.setCookieLabel(_))
        session
    }

    writeOutputdata(sc, trackerDataPath, parseLogRDD, labeledSessionRDD)
  }
  private def writeOutputdata(sc: SparkContext, trackerDataPath: String,
                              parsedLogRDD: RDD[TrackerLog],
                              labeledSessionRDD: RDD[TrackerSession]): Unit ={
    val configuration = sc.hadoopConfiguration
    val trackLogOutputPath = s"${trackerDataPath}/trackerLog/"

    deleteIfExist(trackLogOutputPath, configuration)
    AvroWriteSupport.setSchema(configuration, TrackerLog.SCHEMA$)
    parsedLogRDD.map((null,_)).saveAsNewAPIHadoopFile(trackLogOutputPath,
      classOf[Void], classOf[TrackerLog], classOf[AvroParquetOutputFormat[TrackerLog]], configuration)

    val trackSessionOutputPath = s"${trackerDataPath}/trackerSession/"
    deleteIfExist(trackSessionOutputPath, configuration)

    AvroWriteSupport.setSchema(configuration, TrackerSession.SCHEMA$)
    labeledSessionRDD.map((null,_)).saveAsNewAPIHadoopFile(trackLogOutputPath,
      classOf[Void], classOf[TrackerSession], classOf[AvroParquetOutputFormat[TrackerSession]], configuration)
  }

  def deleteIfExist(trackLogOutputPath: String, configuration: Configuration) = {
    val path = new Path(trackLogOutputPath)
    val fs = path.getFileSystem(configuration)
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
  }

  private def cutSession(sortedParseLogs: Array[TrackerLog]): ArrayBuffer[ArrayBuffer[TrackerLog]] = {
    val sessionParsedLogsBuffer = new ArrayBuffer[TrackerLog]()
    val initBuilder = ArrayBuffer.newBuilder[ArrayBuffer[TrackerLog]]

    val sessionLogResult = sortedParseLogs.foldLeft((initBuilder, Option.empty[TrackerLog])) {
      case ((builder, preLog), currLog) =>
        val currLogTime = dateFormat.parse(currLog.getLogServerTime.toString).getTime
        //超过30分钟，分割为一个新会话
        if (preLog.isDefined && currLogTime - dateFormat.parse(preLog.get.getLogServerTime.toString).getTime > sessionTimeInterval) {
          builder += sessionParsedLogsBuffer.clone()
          sessionParsedLogsBuffer.clear()
        }

        //将当前的log加入到当前的会话中
        sessionParsedLogsBuffer += currLog
        (builder, Some(currLog))
    }._1.result()

    if (sessionParsedLogsBuffer.nonEmpty) {
      sessionLogResult += sessionParsedLogsBuffer
    }

    sessionLogResult
  }

  private def parseRowLog(line: String): Option[TrackerLog] = {
    if (line.startsWith("#")) None
    else {
      val fields = line.split("\\|")
      val trackerLog = new TrackerLog()
      trackerLog.setLogType(fields(0))
      trackerLog.setLogServerTime(fields(1))
      trackerLog.setCookie(fields(2))
      trackerLog.setIp(fields(3))
      trackerLog.setUrl(fields(4))
      Some(trackerLog)
    }

  }
}

//class ClickTrackerKryoRegistrator extends KryoRegistrator {
//  override def registerClasses(kryo: Kryo): Unit = {
//    kryo.register(classOf[TrackerLog])
//    kryo.register(classOf[TrackerSession])
//  }
//}