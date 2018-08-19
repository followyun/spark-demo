package com.my.spark.udf

import com.my.spark.sql.User
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator
/**
  * 有类型的求平均值
  */
case class Average(var sum: Long, var count: Long)

object TypedAverage extends Aggregator[User, Average, Double]{
  //1. 定义数据类型
  override def bufferEncoder: Encoder[Average] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  //初始化中间值
  override def zero: Average = Average(0L, 0L)
  //2.聚合过程
  //处理每一条数据
  override def reduce(b: Average, a: User): Average = {
    b.sum = b.sum + a.age
    b.count = b.count + 1
    b
  }
//合并分区
  override def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count += b2.count

    b1
  }

  //计算结果
  override def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count

}
