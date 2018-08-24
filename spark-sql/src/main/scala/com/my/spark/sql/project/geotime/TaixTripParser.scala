package com.my.spark.sql.project.geotime

import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.spark.sql.Row

/**
  * 解析从csv中读取出来的载客记录
  */
//载客记录
case class Trip(
                 license: String, //出租车司机编号
                 pickupTime: Long, //乘客上车时间
                 dropoffTime: Long, //乘客下车时间
                 pickupX: Double, //乘客上车地点的经度
                 pickupY: Double, //乘客上车地点的纬度
                 dropoffX: Double, //乘客下车地点的经度
                 dropoffY: Double //乘客下车地点的纬度
               )

/**
  * 不会出现空指针异常的Row
  *@param row 原始DataFrame中的Row
  */
class RichRow(row:Row){
  /**
    * 根据字段名从row中获取对应的值
    * @param field  字段名
    * @tparam T  值的类型
    * @return  值   如果在段名不存在或者字段名对应的值为null的话则返回None，否则返回Some(值)
    */
  def getAs[T](field:String): Option[T]={
    if(row.isNullAt(row.fieldIndex(field))) None else Some(row.getAs[T](field))
  }
}

object TaixTripParser {
  /**
    * 解析从csv读取到的一行载客记录
    * @param r
    * @return
    */
  def parse(r: Row): Trip ={
    val row = new RichRow(r)
    Trip(license = row.getAs[String]("hack_license").orNull,
      pickupTime = parseTaxiTime(row, "pickup_datetime"),
      dropoffTime = parseTaxiTime(row, "dropoff_datetime"),
      pickupX = parseTaxiLoc(row, "pickup_longitude"),
      pickupY = parseTaxiLoc(row, "pickup_latitude"),
      dropoffX = parseTaxiLoc(row, "dropoff_longitude"),
      dropoffY = parseTaxiLoc(row, "dropoff_latitude")
    )
  }

  //获取乘客上下车的时间并格式化时间
  def parseTaxiTime(row:RichRow, timeField:String): Long={
    val formatter = new SimpleDateFormat("yyyy/MM/dd HH:mm", Locale.ENGLISH)
    val timeOpt = row.getAs[String](timeField)
    //这里报java.lang.ClassCastException: java.lang.String cannot be cast to scala.runtime.Nothing
    //原因上一句row.getAs[String](timeField) //之前是row.getAs(timeField)并未指定类型String
    timeOpt.map(println)
    timeOpt.map(dt => formatter.parse(dt).getTime).getOrElse(0L)
  }

  //获取乘客上下车的地理位置信息
  def parseTaxiLoc(row:RichRow, localField:String):Double={
    row.getAs[String](localField).map(_.toDouble).getOrElse(0.0)
  }
}
