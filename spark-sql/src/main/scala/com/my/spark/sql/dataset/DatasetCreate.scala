package com.my.spark.sql.dataset

import com.my.spark.sql.Dog
import my.spark.sql.bean.Cat
import org.apache.spark.sql.{Encoders, SparkSession}

/**
  * Dataset的创建
  * 1. sparkSession.range()方法
  * 2. Seq[T]
  * 3. RDD[T]
  * 需要引入import spark.implicits._
  */
object DatasetCreate {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().
      master("local[*]")
      .appName("DatasetCreate")
      .getOrCreate()
    import spark.implicits._ //导入隐式转换
    //1. range()方法
    val rangeDS = spark.range(0, 8, 2, 2) //第四个参数为分区数
    rangeDS.show()

    //2. Seq[T]
    val dogs = Seq(Dog("alasiji", 1),Dog("bunimao", 2))
    val cats = Seq(new Cat("buou", 1), new Cat("coffe", 2))//元素为java bean
    val dogsDS = spark.createDataset(dogs) //== dogs.toDS 使用了隐式转换
    dogsDS.show()
    //Encoders负责jvm中对象类型与spark sql内部数据类型进行转换
    val catsDS = spark.createDataset(cats)(Encoders.bean(classOf[Cat]))//无法识别java的bean，需要使用Encoders进行转换
    catsDS.show()

    //3. RDD[T]
    val dogsRDD = spark.sparkContext.parallelize(dogs)
    val catsRDD = spark.sparkContext.parallelize(cats)
    val rddDogsDS = spark.createDataset(dogsRDD)
    rddDogsDS.show()
    val rddCatsDS = spark.createDataset(catsRDD)(Encoders.bean(classOf[Cat]))
    rddCatsDS.show()


  }
}
