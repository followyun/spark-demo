package com.my.spark.rdd

import org.apache.spark.{HashPartitioner, Partitioner}

/**
  * 自定义分区器
  * 域名分区器
  *
  */
class DomainNamePartitioner(partitions: Int) extends Partitioner{
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    if(key.toString.startsWith("https")){
      1
    }else{
      val code = key.toString.hashCode % partitions
      if(code == 1){
        2
      }else if( code < 0){
        code + partitions
      }else{
        code
      }
    }

  }
  override def equals(other: Any): Boolean = other match {
    case h: DomainNamePartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}
