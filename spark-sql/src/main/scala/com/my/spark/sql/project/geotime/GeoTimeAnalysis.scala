package com.my.spark.sql.project.geotime

import java.util.concurrent.TimeUnit

import com.esri.core.geometry.Point
import org.apache.spark.sql.{Row, SparkSession}
import com.my.spark.util.Utils._
import com.my.spark.sql.project.geotime.GeoJsonProtocol._
import spray.json._

/**
  *载客分析
  * 分析出租车司机载客到一个区下后，多久能再载下一个乘客，推荐合适的区域
  */
object GeoTimeAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("GeoTimeAnalysis").master("local[*]").getOrCreate()
    import spark.implicits._
    //1. 从csv文件中读取出租车载客记录
    val taxiDF = spark.read.option("header", "true").csv(s"${BASIC_PATH}/spark/dataset/trip_data.csv")
    //2. 解析出租车载客记录数据
    //在解析过程中可能会遇到如下异常1，有些字段存在空值；2，有些字段不为空，但解析时出现异常
//    val taxiParsed = taxiDF.rdd.map(safe(TaixTripParser.parse))
    val taxiParsed = taxiDF.rdd.map(safe(TaixTripParser.parse))
    //3. 拿到有效的租客记录，并转成Dataset
    val taxiGood = taxiParsed.map(_.left.get).toDS()

    //4. 对解析之后的数据再次进行业务上异常的过滤
    //比如一次载客时间超过3小时就不是正常的，需要过滤掉
    //4.1 计算载客上车和下车的时间间隔的函数
    val hours = (pickupTime: Long , dropoffTime: Long)=> {
      TimeUnit.HOURS.convert(dropoffTime - pickupTime, TimeUnit.MILLISECONDS)
    }

    //将4.1 函数注册成udf
    spark.udf.register("hours", hours)
    val taxiClean = taxiGood.where("hours(pickupTime, dropoffTime) BETWEEN 0 AND 3")

    //5. 准备纽约市每个区的区地理位置信息
    //读geoJson数据
    val geoJsonStr = scala.io.Source.fromURL(this.getClass.getResource("/nyc-boroughs.geojson")).mkString
    //获得featureCollection对象
    val featureCollection = geoJsonStr.parseJson.convertTo[FeatureCollection]
    //按照区代码和区的面积排序
    val areaSortedFeature = featureCollection.sortBy(f=> {val borought = f("boroughCode").convertTo[Int]
      (borought, -f.geometry.area2D())})
    //将排序好的feature广播到每一个executors上（数据量比较小）
    val bFeatures = spark.sparkContext.broadcast(areaSortedFeature)
    //根据经度和纬度定位所属区的方法，并将这个方法注册为udf
    import org.apache.spark.sql.functions._
    val bLookup = (x: Double, y: Double) =>{
      val feature: Option[Feature] = bFeatures.value.find(f=>f.geometry.contains(new Point(x, y)))

      feature.map(f => {
        f("borough").convertTo[String]
      }).getOrElse("NA")
    }

    val brogouhUDF = udf(bLookup)

    //过滤掉含有无效的经度和纬度载客的记录数据
    val taxiDone = taxiClean.where("dropoffX != 0 and dropoffY != 0 and pickupX != 0 and pickupY != 0")

    //按照出租车进行重新分区，且在每一个分区中先按照出租车进行排序，然后再按照乘车上车的时间进行排序
    val sessions = taxiDone.repartition($"license").
      sortWithinPartitions($"license", $"pickupTime")//二次排序

    //计算一个出租车在一个区载客下车后，隔多长时间才载下一个载客
    def boroughDuration(t1:com.my.spark.sql.project.geotime.Trip, t2:com.my.spark.sql.project.geotime.Trip) :(String, Long) = {
      val b = bLookup(t1.dropoffX, t1.dropoffY)
      val d = (t2.pickupTime - t1.dropoffTime) / 1000
      (b, d)
    }

    //计算所有出租车在一个区下客后，隔多长时间才载下一个游客
    val boroughDurations = sessions.mapPartitions(trips =>{
      val iter: Iterator[Seq[com.my.spark.sql.project.geotime.Trip]] = trips.sliding(2)
      val viter = iter.filter(_.size == 2).filter(p => p(0).license == p(1).license)
      viter.map(p => boroughDuration(p(0), p(1)))
    }).toDF("borough", "seconds")

    //按照区进行聚合，计算出租车在每一个区载客下车后，隔多长时间才能载下一个载客
    boroughDurations.where("seconds > 0 AND  seconds < 60 * 60 * 4").
      groupBy("borough").
      agg(avg("seconds"), stddev("seconds")).show()
  }

  /**
    *  通用的处理异常的方法
    * @param f  函数
    * @tparam S 函数f的输入类型
    * @tparam T 函数f的输出类型
    * @return 返回Either，Left为正常结果，Right为异常结果
    */
  def safe[S, T](f: S => T): S => Either[T, (S, Exception)] = {
    new Function[S, Either[T, (S, Exception)]] with Serializable {
      def apply(s: S): Either[T, (S, Exception)] = {
        try {
          Left(f(s))
        } catch {
          case e: Exception => {
            Right((s, e))}
        }
      }
    }
  }
}
