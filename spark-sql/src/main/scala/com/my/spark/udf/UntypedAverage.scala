package com.my.spark.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * 自定义求平均值的函数(无类型的)
  */
object UntypedAverage extends UserDefinedAggregateFunction{
  //1. 数据类型的定义
  //输入参数数据类型
  override def inputSchema: StructType = StructType(StructField("inputColumn", LongType)::Nil)

  //中间数据的数据类型
  override def bufferSchema: StructType = {
    StructType(StructField("sum", LongType)::StructField("count", LongType)::Nil)
  }

  //输出结果的数据类型
  override def dataType: DataType = DoubleType
  //2：函数的特点
  //当这个聚合函数每次的输入数据是一样的时候，它的输出结果是不是都是一样的
  override def deterministic: Boolean = true

  //3. 聚合过程
  //初始化聚合过程中需要的buffer对象
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  //当来了一条row输入数据的时候，更新聚合函数中的buffer值
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  //合并两个buffer，且将合并之后的值复制给第一个buffer
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}
