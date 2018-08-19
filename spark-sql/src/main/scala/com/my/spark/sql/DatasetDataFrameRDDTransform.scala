package com.my.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Dataset DataFrame RDD之间相互转换
  */
object DatasetDataFrameRDDTransform {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Dataset Test").master("local[*]").getOrCreate()

    val dogs = Seq(Dog("abuladuo", 1), Dog("zhonghuatianyuan", 1))
    val dogsRDD = spark.sparkContext.parallelize(dogs)

    import spark.implicits._
    //Dataset与RDD之间的互转
    val dogsDS = dogsRDD.toDS()
    dogsDS.show()

    val dogsRDD1:RDD[Dog] = dogsDS.rdd //rdd数据类型为DS中存放的数据类型

    //DataFrame与RDD之间的互转
    val dogsDF = dogsRDD.toDF()
    //重新更换名字
    val renameSchemaDogsDF = dogsRDD.toDF("new_name", "new_age")
    renameSchemaDogsDF.show()

    val dogsRDD2:RDD[Row] = dogsDF.rdd //rdd数据类型为Row

    //DataFrame与Dataset之间的互转
    val dogsDSFromDF = dogsDF.as[Dog]
    val dogsDFFromDS = dogsDS.toDF()
  }
}
