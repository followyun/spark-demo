package com.my.spark.streaming.output

import java.sql.DriverManager

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 保存streaming计算结果到mysql中
  * spark-shell --total-executor-cores 4 --executor-cores 2 --master spark://master:7077 --jars mysql-connector-java-5.1.44-bin.jar,c3p0-0.9.1.2.jar,spark-streaming-1.0-SNAPSHOT-jar-with-dependencies.jar
  *
  */
object SaveToMysqlApi {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("NetWorkWordCount")
    val sc = new SparkContext(conf)

    //创建spark streaming编程入口，处理间隔为1S
    val ssc = new StreamingContext(sc, Seconds(1))
    //创建一个接收器，接收master服务器发送过来的数据
    val lines = ssc.socketTextStream("master", 9998, StorageLevel.MEMORY_AND_DISK_SER_2)

    val words = lines.flatMap(_.split(" "))
    val wordsPairs = words.map((_, 1))
    val wordsCount = wordsPairs.reduceByKey(_ + _)
    //将数据保存到Mysql中
    //保存方式一
    //错误 1，在executor端使用了driver端的代码(conn, statement)
    //2. 没有使用事务
    //3. 没有使用连接池
    //4. 也没有对数据提交实行批处理
    wordsCount.foreachRDD((rdd, time) =>{
      Class.forName("com.mysql.jdbc.Driver")
      val conn = DriverManager.getConnection("jdbc:mysql://master:3306/test", "root", "12345678")
      val statement = conn.prepareStatement("insert into word_count(ts, word, count) values (?, ?, ?)")
      rdd.foreach{record =>
        statement.setLong(1, time.milliseconds)
        statement.setString(2, record._1)
        statement.setInt(3, record._2)
        statement.execute()
      }
      statement.close()
      conn.close()
    })

    //保存方式二
    wordsCount.foreachRDD{(rdd, time) =>
      rdd.foreachPartition{partitionRecords=>
        val conn = ConnectionPool.getConnection
        conn.setAutoCommit(false)
        val statement = conn.prepareStatement(s"insert into word_count(ts, word, count) values (?, ?, ?)")
        partitionRecords.zipWithIndex.foreach{
          case ((word, count), index)=>
            statement.setLong(1, time.milliseconds)
            statement.setString(2, word)
            statement.setInt(3, count)
            statement.addBatch()//加入到批处理中

          if(index != 0 && index % 500 == 0){//批处理满500再进行处理
            statement.executeBatch()
            conn.commit()
          }
        }
        statement.executeBatch()
        statement.close()
        conn.commit()
        conn.setAutoCommit(true)
        ConnectionPool.returnConnection(conn)//将连接返还到连接池
      }
    }

    //启动streaming应用
    ssc.start()

    //等待程序运行结束
    ssc.awaitTermination()

    ssc.stop(false) //参数为是否关闭sparkcontext， 默认为关闭
  }
}
