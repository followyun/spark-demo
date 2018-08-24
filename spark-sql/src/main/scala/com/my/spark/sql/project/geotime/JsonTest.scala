package com.my.spark.sql.project.geotime

/**
  * 测试scala json工具包, spray
  */
object JsonTest {
  def main(args: Array[String]): Unit = {
    val jsonStr = scala.io.Source.fromURL(this.getClass.getResource("/test.json")).mkString
    import spray.json._
    //获取到json对象
    val jsonObject = jsonStr.parseJson.asJsObject

    //需要导入隐式装换
    //import spray.json.DefaultJsonProtocol._
    import MyJsonProtocol._

    //获取到json对象中的某个属性, convertTo方法需要隐式参数JsonReader
    val typeStr = jsonObject.fields.get("typeStr").get.convertTo[String]
    println("typeStr = "+ typeStr)

    //获取类型为Array的数据
    val jsValueArray = jsonObject.fields.get("features").get.convertTo[Array[JsValue]]

    //获取Array并指定元素类型
    val featureArray = jsonObject.fields.get("features").get.convertTo[Array[MyFeature]]
    println(featureArray)

    //将整个json装换为对象
    jsonObject.convertTo[MyFeatureCollection]
  }
}
