package com.my.spark.project.ncdc

import com.esotericsoftware.kryo.Kryo
import com.my.rdd.{NcdcRecord, StationMetaData, WeatherData}
import com.my.spark.util.HDFSUtil
import org.apache.parquet.avro.{AvroParquetOutputFormat, AvroParquetWriter, AvroWriteSupport}
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 初始化ncdc数据
  * spark-submit \
    --class com.my.spark.project.ncdc.NcdcDataInit \
    --master spark://master:7077 \
    --deploy-mode client \
    --executor-memory 500m \
    --num-executors 2 \
    --jars parquet-avro-1.8.1.jar \
    --conf spark.session.groupBy.numPartitions=2 \
    --conf spark.tracker.NcdcPath=hdfs://master:9999/user/bd-cqr/ncdc \
    spark-core-1.0-SNAPSHOT.jar \
    nonLocal
  */
object NcdcDataInit {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setAppName("NcdcDataInit")
    if (args.size == 0) {
      config.setMaster("local")
    }
    config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    config.set("spark.kryo.registrator", "com.my.spark.project.userbehaviour.ClickTrackerKryoRegistrator")

    val sc = new SparkContext(config)
    val ncdcPath = config.get("spark.tracker.NcdcPath", "F:\\BigData\\workspace\\spark-demo\\spark-core\\src\\main\\resources\\")

    val weatherDataHadoopFileRDD = sc.textFile(s"${ncdcPath}/rawdata/records/2017.all")
    val weatherDataRDD = weatherDataHadoopFileRDD.map(line => parseWeatherData(line)).filter(_._1 != null)
    val metaDataHadoopFileRDD = sc.textFile(s"$ncdcPath/rawdata/station_metadata/isd-history.csv")
    val metaDataRDD = metaDataHadoopFileRDD.map(parseStationMetadata(_)).filter(_ != null)

    val ncdcRecordRDD = weatherDataRDD.leftOuterJoin(metaDataRDD).map[NcdcRecord](data=>constructNcdcRecord(data._2._1, data._2._2))

    val ncdcRecordPath = s"${ncdcPath}/initedData/"
    HDFSUtil.deleteFileIfExisted(ncdcRecordPath, sc.hadoopConfiguration)
    AvroWriteSupport.setSchema(sc.hadoopConfiguration, NcdcRecord.SCHEMA$)
    println(s"开始保存ncdcrecord数据到${ncdcRecordPath}中")
    ncdcRecordRDD.map((null,_)).saveAsNewAPIHadoopFile(ncdcRecordPath, classOf[Void], classOf[NcdcRecord], classOf[AvroParquetOutputFormat[NcdcRecord]], sc.hadoopConfiguration)

  }



  /**
    * 解析一行气象数据
    * 每行数据代表含义见
    * STN--- WBAN   YEARMODA    TEMP       DEWP      SLP        STP       VISIB      WDSP     MXSPD   GUST    MAX     MIN   PRCP   SNDP   FRSHTT
    * 012450 99999  20180101    29.9 24    20.1 24   981.4 24   912.0 24  999.9  0    4.6 24    9.7  999.9    34.0    24.6   0.00G 999.9  000000
    *
    * @param line
    * @return
    */
  private def parseWeatherData(line: String) : (String, WeatherData)= {
    //println("解析weatherData...")
    //if (line == null || "" == line || line.startsWith("STN"))(null, null)
    if(line != null && "" != line && !line.startsWith("STN")) {
      val data = new WeatherData
      data.setStationId(line.substring(0, 6) + "-" + line.substring(7, 12))
      data.setYear(line.substring(14, 18))
      data.setMonth(line.substring(14, 20))
      data.setDay(line.substring(14, 22))

      data.setMeanTemp(line.substring(24, 30).trim.toDouble)
      data.setMeanTempCount(line.substring(31, 33).trim.toInt)
      data.setMeanDewPointTemp(line.substring(35, 41).trim.toDouble)
      data.setMeanDewPointTempCount(line.substring(42, 44).trim.toInt)
      data.setMeanSeaLevelPressure(line.substring(46, 52).trim.toDouble)
      data.setMeanSeaLevelPressureCount(line.substring(53, 55).trim.toInt)
      data.setMeanStationPressure(line.substring(57, 63).trim.toDouble)
      data.setMeanStationPressureCount(line.substring(64, 66).trim.toInt)
      data.setMeanVisibility(line.substring(68, 73).trim.toDouble)
      data.setMeanVisibilityCount(line.substring(74, 76).trim.toInt)
      data.setMeanWindSpeed(line.substring(78, 83).trim.toDouble)
      data.setMeanWindSpeedCount(line.substring(84, 86).trim.toInt)
      data.setMaxSustainedWindSpeed(line.substring(88, 93).trim.toDouble)
      data.setMaxGustWindSpeed(line.substring(95, 100).trim.toDouble)
      data.setMaxTemp(line.substring(102, 108).trim.toDouble)
      data.setMaxTempFlag(line.substring(108, 109).trim)
      data.setMinTemp(line.substring(110, 116).trim.toDouble)
      data.setMinTempFlag(line.substring(116, 117))

      data.setTotalPrecipitation(line.substring(118, 123).trim.toDouble)
      data.setTotalPrecipitationFlag(line.substring(123, 124))

      data.setSnowDepth(line.substring(125, 130).trim.toDouble)

      val indicators = line.substring(132, 138).toCharArray
      data.setHasFog(fromDigit(indicators(0)))
      data.setHasRain(fromDigit(indicators(1)))
      data.setHasSnow(fromDigit(indicators(2)))
      data.setHasHail(fromDigit(indicators(3)))
      data.setHasThunder(fromDigit(indicators(4)))
      data.setHasTornado(fromDigit(indicators(5)))

      (data.getStationId.toString, data)
    }else{
      (null, null)
    }
  }

  /**
    * 解析一行气象站元数据
    *
    * @param lineData
    * @return
    */
  private def parseStationMetadata(lineData: String): (String, StationMetaData) = {
    //println("解析stationMetaData...")
    if(lineData == null ||"" == lineData || lineData.startsWith("\"USAF\"" )) null
    val values = lineData.split(",")
    val metaData = new StationMetaData
    metaData.setStationId(values(0).replace("\"", "") + "-" + values(1).replace("\"", ""))
    metaData.setStationName(values(2).replace("\"", ""))
    metaData.setStationCity(values(3).replace("\"", ""))
    metaData.setStationState(values(4).replace("\"", ""))
    metaData.setStationICAO(values(5).replace("\"", ""))
    metaData.setStationLatitude(values(6).replace("\"", ""))
    metaData.setStationLongitude(values(7).replace("\"", ""))
    metaData.setStationElev(values(8).replace("\"", ""))
    metaData.setStationBeginTime(values(9).replace("\"", ""))
    metaData.setStationEndTime(values(10).replace("\"", ""))

    (metaData.getStationId.toString, metaData)
  }

  private def constructNcdcRecord(weatherData: WeatherData, metaData: Option[StationMetaData]): NcdcRecord = {
    println("解析ncdcRecord...")
    val ncdcRecord = new NcdcRecord()
    //add  stationMetaData
    if (metaData.isDefined) {
      val metaDataVal = metaData.get
      ncdcRecord.setStationId(metaDataVal.getStationId)
      ncdcRecord.setStationCity(metaDataVal.getStationCity)
      ncdcRecord.setStationState(metaDataVal.getStationState)
      ncdcRecord.setStationICAO(metaDataVal.getStationICAO)
      ncdcRecord.setStationLatitude(metaDataVal.getStationLatitude)
      ncdcRecord.setStationLongitude(metaDataVal.getStationLongitude)
      ncdcRecord.setStationElev(metaDataVal.getStationElev)
      ncdcRecord.setStationBeginTime(metaDataVal.getStationBeginTime)
      ncdcRecord.setStationEndTime(metaDataVal.getStationEndTime)
      ncdcRecord.setStationName(metaDataVal.getStationName)
    }

    //add weatherData
    ncdcRecord.setYear(weatherData.getYear)
    ncdcRecord.setMonth(weatherData.getMonth)
    ncdcRecord.setDay(weatherData.getDay)

    ncdcRecord.setMeanTemp(weatherData.getMeanTemp)
    ncdcRecord.setMeanTempCount(weatherData.getMeanTempCount)
    ncdcRecord.setMeanDewPointTemp(weatherData.getMeanDewPointTemp)
    ncdcRecord.setMeanDewPointTempCount(weatherData.getMeanDewPointTempCount)
    ncdcRecord.setMeanSeaLevelPressure(weatherData.getMeanSeaLevelPressure)
    ncdcRecord.setMeanSeaLevelPressureCount(weatherData.getMeanStationPressureCount)
    ncdcRecord.setMeanStationPressure(weatherData.getMeanStationPressure)
    ncdcRecord.setMeanStationPressureCount(weatherData.getMeanStationPressureCount)
    ncdcRecord.setMeanVisibility(weatherData.getMeanVisibility)
    ncdcRecord.setMeanVisibilityCount(weatherData.getMeanVisibilityCount)
    ncdcRecord.setMeanWindSpeed(weatherData.getMeanWindSpeed)
    ncdcRecord.setMeanWindSpeedCount(weatherData.getMeanWindSpeedCount)
    ncdcRecord.setMaxSustainedWindSpeed(weatherData.getMaxSustainedWindSpeed)
    ncdcRecord.setMaxGustWindSpeed(weatherData.getMaxGustWindSpeed)
    ncdcRecord.setMaxTemp(weatherData.getMaxTemp)
    ncdcRecord.setMaxTempFlag(weatherData.getMaxTempFlag)
    ncdcRecord.setMinTemp(weatherData.getMinTemp)
    ncdcRecord.setMinTempFlag(weatherData.getMinTempFlag)

    ncdcRecord.setTotalPrecipitation(weatherData.getTotalPrecipitation)
    ncdcRecord.setTotalPrecipitationFlag(weatherData.getTotalPrecipitationFlag)
    ncdcRecord.setSnowDepth(weatherData.getSnowDepth)

    ncdcRecord.setHasFog(weatherData.getHasFog)
    ncdcRecord.setHasRain(weatherData.getHasRain)
    ncdcRecord.setHasSnow(weatherData.getHasSnow)
    ncdcRecord.setHasHail(weatherData.getHasHail)
    ncdcRecord.setHasThunder(weatherData.getHasThunder)
    ncdcRecord.setHasTornado(weatherData.getHasTornado)

    ncdcRecord
  }

  private def fromDigit(digit: Char) = {
    if ('0' == digit)
      false
    else
      true
  }

}

class ClickTrackerKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[WeatherData])
    kryo.register(classOf[StationMetaData])
    kryo.register(classOf[NcdcRecord])
  }
}
