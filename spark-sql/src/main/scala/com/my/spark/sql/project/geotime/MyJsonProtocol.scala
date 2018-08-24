package com.my.spark.sql.project.geotime

import spray.json.{DefaultJsonProtocol, JsNumber, JsObject, JsString, JsValue, RootJsonFormat}

import scala.collection.mutable.ArrayBuffer
import spray.json._
/**
  * 自定义json处理协议，处理Feature和FeatureCollection类型数据的json转化
  */
case class MyFeature(val typeStr:String, val id:Int, val name:Option[String])
case class MyFeatureCollection(val typeStr:String, val features: Array[MyFeature])
object MyJsonProtocol extends DefaultJsonProtocol{
    implicit object FeatureJsonFormat extends RootJsonFormat[MyFeature] {
      //将JsValue转换为Feature对象
      override def read(json: JsValue): MyFeature = {
        val jsObject = json.asJsObject
        val typeStr = jsObject.fields.get("typeStr").get.convertTo[String]
        val id = jsObject.fields.get("id").get.convertTo[Int]
        val name = jsObject.fields.get("name").map(_.convertTo[String])

        MyFeature(typeStr,id,name)
      }
      //将Feature对象转换为JsValue
      override def write(obj: MyFeature): JsObject = {
        val buf = ArrayBuffer("typeStr" ->JsString(obj.typeStr),
          "id"->JsNumber(obj.id))

          obj.name.foreach(n=>buf += "name"->JsString(n))

          JsObject(buf.toMap)
      }
    }

  implicit object FeatureCollectionJsonFormat extends RootJsonFormat[MyFeatureCollection] {
    override def read(json: JsValue): MyFeatureCollection = {
      val jsonObject = json.asJsObject
      val typeStr = jsonObject.fields.get("typeStr").get.convertTo[String]
      val features = jsonObject.fields.get("features").get.convertTo[Array[MyFeature]]

      MyFeatureCollection(typeStr, features)
    }

    override def write(obj: MyFeatureCollection): JsObject = {
      JsObject("typeStr"->JsString(obj.typeStr), "features"->JsArray(obj.features.map(_.toJson): _*))
    }

  }
}
