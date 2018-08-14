package com.my.spark.project.userbehaviour

import java.beans.Transient
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
  * 网站用户行为分析
  */
/**
  * 提交spark应用的方法
   spark-submit \
  --class com.my.spark.project.userbehaviour.UserClickTrackETL \
  --master spark://master:7077 \
  --deploy-mode client \
  --executor-memory 500m \
  --num-executors 2 \
  --jars parquet-avro-1.8.1.jar \
  --conf spark.session.groupBy.numPartitions=2 \
  --conf spark.tracker.trackerDataPath=hdfs://master:9999/user/bd-cqr/spark/userbehaviour \
  spark-core-1.0-SNAPSHOT.jar \
nonLocal
  */
/**
  * 验证spark输出
  * val log = spark.read.parquet(“hdfs://master:9999/xxx/trackerLog”)
log.show
val session = spark.read.parquet(”hdfs://master:9999/xxx/trackerSession”)
session.show

  */
object UserClickTrackETL {
  private val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  private val sessionTimeInterval = 30 * 60 * 1000
  private val logTypeSet = Set("click", "pageview")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UserClickTrackETL")
    if (args.size == 0) {
      conf.setMaster("local")
    }

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "com.my.spark.project.userbehaviour.ClickTrackerKryoRegistrator")

    val sc = new SparkContext(conf)
    val trackerDataPath = conf.get("spark.tracker.trackerDataPath", "F:\\BigData\\workspace\\spark-demo\\spark-core\\src\\main\\resources\\")
    val numPartitions = conf.getInt("spark.session.groupBy.numPartitions", 10)

    //网站域名标签
    val domainLabelMap = Map("www.baidu.com" -> "level1", "www.ali.com" -> "level2",
      "jd.com" -> "level3", "youku.com" -> "level4")

    //将标签广播到每一个机器上
    val labelMapB = sc.broadcast(domainLabelMap)

    //统计一共生成了多少个会话
    val sessionCountAccumulator = sc.longAccumulator("session count")

    //从hdfs中获取原始数据
    val rowLogRDD = sc.textFile(s"${trackerDataPath}/visit_log.txt")
    //对每一条原始日志进行解析成TrackerLog对象，并过滤掉logType不是"click", "pageview"的日志
    val parsedLogRDD = rowLogRDD.flatMap(line => parseRowLog(line))
      .filter(rowLog => logTypeSet.contains(rowLog.getLogType.toString)) //过滤
      .persist(StorageLevel.DISK_ONLY) //存放到硬盘

    val partitioner = new HashPartitioner(numPartitions)
    //获取hdfs中的cookie_label.txt
    val cookieLabelRDD = sc.textFile(s"${trackerDataPath}/cookie_label.txt");
    val cLArray = cookieLabelRDD.map(line => {
      val labels = line.split("\\|")
      labels(0) -> labels(1)
    }).collect()

    val cLArrayB = sc.broadcast(cLArray); //cLArray广播到每个executor

    val sessionRDD = parsedLogRDD.groupBy((rowLog: TrackerLog) => rowLog.getCookie.toString, partitioner).flatMapValues {
      case iter =>
        //对每一个cookie下的日志按照日志的时间升序排序
        val sortedParseLogs = iter.toArray.sortBy(_.getLogServerTime.toString)
        //对每一个cookie下的日志进行遍历，按30分钟切割会话
        val sessionParsedLogsResult = cutSession(sortedParseLogs)

        println(s"sessionParsedLogsResult=${sessionParsedLogsResult.toList}")
        sessionParsedLogsResult.map(session => {
          println(s"labelMapB.value=${labelMapB.value}")
          println(s"session=${session}")
          session.setDomainLabel(labelMapB.value.getOrElse(session.getDomain.toString, "-")) //域名标签
          val label = cLArrayB.value.find(_._1 == session.getCookie.toString).get._2
          session.setCookieLabel(label) //设置cookie标签
          sessionCountAccumulator.add(1)
          session
        })
    }
    //将计算结果保存到hdfs中
    val finalSessionRDD = sessionRDD.map(_._2)

    writeOutputData(sc, trackerDataPath, parsedLogRDD, finalSessionRDD)

  }

  def cutSession(parseLogs: Array[TrackerLog]): Array[TrackerSession] = {
    val sessions = new ArrayBuffer[TrackerSession]();
    var trackerSession: TrackerSession = null;
    var firstLog: TrackerLog = parseLogs(0);
    for (log <- parseLogs) {
      val firstTime = dateFormat.parse(firstLog.getLogServerTime.toString).getTime
      val currentTime = dateFormat.parse(log.getLogServerTime.toString).getTime
      val thisSessionTimeInterval = (currentTime - firstTime) / (1000 * 60)
      if (thisSessionTimeInterval <= sessionTimeInterval && parseLogs.indexOf(log) != 0) {
        //如果是在一个会话中
        log.getLogType match {
          case "click" => trackerSession.setClickCount(trackerSession.getClickCount + 1);
          case "pageview" => trackerSession.setPageviewCount(trackerSession.getPageviewCount + 1);
          case _ => ;
        }
        trackerSession.setSessionServerTime(thisSessionTimeInterval.toString)
      } else {
        firstLog = log
        trackerSession = trackerSessionInit(log, firstLog)
        sessions += trackerSession


      }
    }
    sessions.toArray

  }

  private def trackerSessionInit(trackerLog: TrackerLog, firstLog: TrackerLog): TrackerSession = {
    val trackerSession = new TrackerSession()
    firstLog.getLogType match {
      case "click" => trackerSession.setClickCount(1);
      case "pageview" => trackerSession.setPageviewCount(1);
      case _ => ;
    }
    trackerSession.setCookie(firstLog.getCookie)
    trackerSession.setDomain(new URL(firstLog.getUrl.toString).getHost)
    trackerSession.setIp(firstLog.getIp)
    trackerSession.setLandingUrl(firstLog.getUrl)
    trackerSession.setSessionId(UUID.randomUUID().toString)

    trackerSession
  }

  private def writeOutputData(sc: SparkContext, trackerDataPath: String, logRDD: RDD[TrackerLog], sessionRDD: RDD[TrackerSession]): Unit = {
    val configuration = sc.hadoopConfiguration
    val trackerLogOutputPath = s"${trackerDataPath}/trackerLog/"
    deleteIfExist(trackerLogOutputPath, configuration)

    //将trackerLog保存为parquet文件
    println("将trackerLog保存为parquet文件")
    AvroWriteSupport.setSchema(configuration, TrackerLog.SCHEMA$)
    logRDD.map((null, _)).saveAsNewAPIHadoopFile(trackerLogOutputPath, classOf[Void], classOf[TrackerLog], classOf[AvroParquetOutputFormat[TrackerLog]], configuration)

    val trackerSessionOutputPath = s"${trackerDataPath}/trackerSession/"
    deleteIfExist(trackerSessionOutputPath, configuration)

    //将trackerSession保存为parquet文件
    println("将trackerSession保存为parquet文件")
    AvroWriteSupport.setSchema(configuration, TrackerSession.SCHEMA$)
    sessionRDD.map((null, _)).saveAsNewAPIHadoopFile(trackerSessionOutputPath, classOf[Void], classOf[TrackerSession], classOf[AvroParquetOutputFormat[TrackerSession]], configuration)
  }

  private def deleteIfExist(hdfsPath: String, configuration: Configuration) = {
    val path = new Path(hdfsPath)
    val fs = path.getFileSystem(configuration)
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
  }

  private def parseRowLog(line: String): Option[TrackerLog] = {
    if (line.startsWith("2#")) {
      //过滤掉字段说明
      None
    }

    val datas = line.split("\\|")
    val log = new TrackerLog()
    log.setCookie(datas(2))
    log.setIp(datas(3))
    log.setLogServerTime(datas(1))
    log.setLogType(datas(0))
    log.setUrl(datas(4))

    Some(log)

  }
}

class ClickTrackerKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[TrackerLog])
    kryo.register(classOf[TrackerSession])
  }

}
