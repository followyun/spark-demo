package com.my.spark.sql.project.geotime

import com.esri.core.geometry.{GeoJsonImportFlags, Geometry, GeometryEngine}
import spray.json.{DefaultJsonProtocol, JsValue, RootJsonFormat}
import spray.json._
/**
  * Geometry类型数据与json之间的转换
  */
case class Feature(id: Option[JsValue],
                   properties: Map[String, JsValue],
                   geometry: RichGeometry) {
  def apply(property: String): JsValue = properties(property)
  def get(property: String): Option[JsValue] = properties.get(property)
}
case class FeatureCollection(features: Array[Feature])
  extends IndexedSeq[Feature] {
  def apply(index: Int): Feature = features(index)
  def length: Int = features.length
}
object GeoJsonProtocol extends DefaultJsonProtocol{
  implicit object RichGeometryJsonFormat extends RootJsonFormat[RichGeometry] {
    override def read(json: JsValue): RichGeometry = {
      val mg = GeometryEngine.geometryFromGeoJson(json.compactPrint,
        GeoJsonImportFlags.geoJsonImportDefaults, Geometry.Type.Unknown)

      new RichGeometry(mg.getGeometry, mg.getSpatialReference)
    }

    override def write(obj: RichGeometry): JsValue = {
      GeometryEngine.geometryToGeoJson(obj.spatialReference, obj.geometry).parseJson
    }
  }

  implicit object FeatureJsonFormat extends RootJsonFormat[Feature] {
    def write(f: Feature): JsObject = {
      val buf = scala.collection.mutable.ArrayBuffer(
        "type" -> JsString("Feature"),
        "properties" -> JsObject(f.properties),
        "geometry" -> f.geometry.toJson)
      f.id.foreach(v => { buf += "id" -> v})
      JsObject(buf.toMap)
    }

    def read(value: JsValue): Feature = {
      val jso = value.asJsObject
      val id = jso.fields.get("id")
      val properties = jso.fields("properties").asJsObject.fields
      val geometry = jso.fields("geometry").convertTo[RichGeometry]
      Feature(id, properties, geometry)
    }
  }

  implicit object FeatureCollectionJsonFormat extends RootJsonFormat[FeatureCollection] {
    override def read(json: JsValue): FeatureCollection = {
      val featureArray = json.asJsObject.fields("features").convertTo[Array[Feature]]
      FeatureCollection(featureArray)
    }

    override def write(obj: FeatureCollection): JsValue = {
      JsObject("type"->JsString("FeatureCollection"),
        "features"->JsArray(obj.features.map(_.toJson):_*)
      )
    }
  }
}
