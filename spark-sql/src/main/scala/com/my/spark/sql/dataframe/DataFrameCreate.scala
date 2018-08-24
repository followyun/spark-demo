package com.my.spark.sql.dataframe

import com.my.spark.sql.Dog
import my.spark.sql.bean.Cat
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
/**
  *创建dataframe:
  *从RDD[A <: Product]中创建
  *从Seq[A <: Product]中创建
  *用RDD[_] + class创建
  *用RDD[Row] + schema创建
  *从外部数据源中创建
  *
*/
object DataFrameCreate {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrameCreate")
      .master("local[*]")
        .getOrCreate()
    //1. 从RDD[A<:Product]中创建,case class和tuple都是Product子集
    val tupleDogRDD = spark.sparkContext.parallelize(Seq(("erha", 2), ("taidi", 1)))

    val dogRDD = tupleDogRDD.map(dog => Dog(dog._1, dog._2))
    //創建dataFrame
    val dogDF = spark.createDataFrame(dogRDD)
//    +-----+---+
//    | name|age|
//    +-----+---+
//    | erha|  2|
//      |taidi|  1|
//      +-----+---+
    dogDF.show()

    val tupleDogDF = spark.createDataFrame(tupleDogRDD)
    tupleDogDF.show()

    //2. 从Seq[A<:Product]中创建
    val seqDF = spark.createDataFrame(Seq(Dog("alasiji", 1),Dog("bunimao", 2)))
    seqDF.createOrReplaceTempView("seqDF")
//    |alasiji|
//    |bunimao|
//    +-------+
    spark.sql("select name from seqDF").show()

    //3. 用RDD[_] + class创建 ，注意class需要实现Serializable接口
    val catJavaBeanRDD = spark.sparkContext.parallelize(Seq(new Cat("buou", 1), new Cat("coffe", 2)))
    val catDF = spark.createDataFrame(catJavaBeanRDD, classOf[Cat])
    catDF.show()

    //4. 用RDD[Row]加schema创建
    val rowDogRDD = spark.sparkContext.parallelize(Seq(("erha", 2), ("taidi", 1)))
      .map(d=>Row(d._1, d._2))
    //创建schema
    val schema = StructType(StructField("name", StringType, false)
      ::StructField("age",IntegerType, true)
      ::Nil)
    val rowDogDF = spark.createDataFrame(rowDogRDD, schema)
//    root
//    |-- name: string (nullable = false)
//    |-- age: integer (nullable = true)
    rowDogDF.printSchema()
    rowDogDF.show()

    //5. 从外部数据源获取数据
    //val fileDogDF = spark.read.json(s"data/sql/te.json")
    val fileDogDF = spark.read.json(s"hdfs://master:9999/user/bd-cqr/person.json")
    fileDogDF.show()

  }
}
