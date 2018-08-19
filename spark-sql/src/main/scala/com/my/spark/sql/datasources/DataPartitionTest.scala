package com.my.spark.sql.datasources

import org.apache.spark.sql.SparkSession
import com.my.spark.util.Utils._
import org.apache.commons.lang3.time.FastDateFormat

/**
  * 数据分区测试
  * 存储时将数据进行缓存，课题优化数据
  */
object DataPartitionTest {
  private val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]")
      .appName("DataPartitionTest").getOrCreate()
    import spark.implicits._
    val sessionDF = spark.read.parquet(s"${BASIC_PATH}/spark/userbehaviour/trackerLog")
    sessionDF.show()
    //按2个字段进行分区
    sessionDF.write.partitionBy("log_type", "cookie").save(s"${BASIC_PATH}/spark/userbehaviour/trackerLog_partition")
    /**
      * drwxr-xr-x	bd-cqr	supergroup	0 B	2018/8/19 上午10:48:40	0	0 B	log_type=click
      * drwxr-xr-x	bd-cqr	supergroup	0 B	2018/8/19 上午10:48:39	0	0 B	log_type=pageview
      */
    val partitionDF = spark.read.parquet(s"${BASIC_PATH}/spark/userbehaviour/trackerLog_partition")
    //用sql来查询log_type=click的数据
    partitionDF.createOrReplaceTempView("trackLogWithPartition")
    spark.sql("select * from trackLogWithPartition where log_type = 'click'").show()

    //直接读取分区对应的文件夹
    val partitionDF1 = spark.read.parquet(s"${BASIC_PATH}/spark/userbehaviour/trackerLog_partition/log_type=click")
    partitionDF1.show()
    //对于hive表可以使用bucket
    //bucket只能用于hive表中
    //而且只用于parquet、json和orc文件格式的文件数据
    sessionDF.write.bucketBy(3, "cookie").saveAsTable("session")
    spark.catalog.dropTempView("trackLogWithPartition")
    spark.catalog.dropGlobalTempView("session")
  }
}
