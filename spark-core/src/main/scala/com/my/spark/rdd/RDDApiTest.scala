package com.my.spark.rdd


import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import java.util.concurrent.TimeUnit

/**
  */
object RDDApiTest {
  val conf = new SparkConf().setAppName("PartitionerTest")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {

  }

  /**
    * 测试map Api
    */
  def test1(): Unit = {

    val listRDD = sc.parallelize(Seq(1,2,3,4,5), 2)
    listRDD.map(x=>{TimeUnit.SECONDS.sleep(2)
      println("sleep...")
      x + 1
    })

    listRDD.mapPartitions(iterator=>{TimeUnit.SECONDS.sleep(2)
      //和map api的功能是一样，只不过map是将函数应用到每一条记录，而这个是将函数应用到每一个partition
      //如果有一个比较耗时的操作，只需要每一分区执行一次这个操作就行，则用这个函数
      //这个耗时的操作可以是连接数据库等操作，不需要计算每一条时候去连接数据库，一个分区只需连接一次就行
      println("sleep...")
      iterator.map(_ + 1)
    })

    listRDD.mapPartitionsWithIndex((index,iterator)=>{TimeUnit.SECONDS.sleep(2) //index为partition的序号
      println(s"sleep...${index}")
      iterator.map(_ + 1)
    })
  }

  /**
    * 测试采样api
    * sample, takeSample, randomSplit
    */
  def test2(): Unit ={

    val listRDD = sc.parallelize(Seq(1,2,3,4), 2)
    //第一个参数为withReplacement
    //如果withReplacement=true的话表示有放回的抽样，采用泊松抽样算法实现
    //如果withReplacement=false的话表示无放回的抽样，采用伯努利抽样算法实现

    //第二个参数为：fraction，表示每一个元素被抽取为样本的概率，并不是表示需要抽取的数据量的因子
    //比如从100个数据中抽样，fraction=0.2，并不是表示需要抽取100 * 0.2 = 20个数据，
    //而是表示100个元素的被抽取为样本概率为0.2;样本的大小并不是固定的，而是服从二项分布
    //当withReplacement=true的时候fraction>=0
    //当withReplacement=false的时候 0 < fraction < 1
    val sampleRDD = listRDD.sample(false, 0.5, 100) //对每个分区进行抽样
    //按照权重对RDD进行随机抽样切分，有几个权重就切分成几个RDD
    val splitRDD = listRDD.randomSplit(Array(0.2, 0.8))//将分区随机划分为n个分区
    val takeRDD = listRDD.takeSample(false, 5) //对整个RDD进行抽样
    splitRDD.size //2
    splitRDD(0).glom().collect()
    splitRDD(1).glom().collect()

    /*
    分层抽样
     */
    val pairRDD = sc.parallelize[(Int, Int)](Seq((1, 2), (3, 4), (3, 6), (5, 6)), 4)

    //分层采样
    val fractions = Map(1 -> 0.3, 3 -> 0.6, 5 -> 0.3)
    val sampleByKeyRDD = pairRDD.sampleByKey(true, fractions)
    sampleByKeyRDD.glom().collect()

    val sampleByKeyExac = pairRDD.sampleByKeyExact(true, fractions)
    sampleByKeyExac.glom().collect()
  }

  /**
    * pipe测试
    */
  def test3(): Unit ={
    val dataRDD = sc.parallelize(List("hi", "hello", "how", "are", "you"), 2)
    //运行进程需要的环境变量
    val env = Map("env" -> "test-env")
    //在执行一个分区task且处理分区输入元素之前将这个分区的全局数据作为脚本的输入的话，则需要定义这个函数
    def printPipeContext(func: String => Unit): Unit = {
      val taskContextData = "this is task context data per partition"
      func(taskContextData)
    }

    //在执行分区task的时候，需要对每一个输入元素做特殊处理的话，可以定义这个函数参数
    def printRDDElement(ele: String, func: String => Unit): Unit = {
      if (ele == "hello") {
        func("dog")
      } else func(ele)
    }


    //表示执行一个本地脚本(可以是shell，python，java等各种能通过java的Process启动起来的脚本进程)
    //dataRDD的数据就是脚本的输入数据，脚本的输出数据会生成一个RDD即pipeRDD
    val pipeRDD = dataRDD.pipe(Seq("python", "/home/hadoop-twq/spark-course/echo.py"),
      env, printPipeContext, printRDDElement, false)
    pipeRDD.glom().collect()

    val pipeRDD2 = dataRDD.pipe("sh /home/hadoop-twq/spark-course/echo.sh")
    pipeRDD2.glom().collect()



    // 你的python脚本所在的hdfs上目录
    // 然后将python目录中的文件代码内容全部拿到
    val scriptsFilesContent =
    sc.wholeTextFiles("hdfs://master:9999/users/hadoop-twq/pipe").collect()
    // 将所有的代码内容广播到每一台机器上
    val scriptsFilesB = sc.broadcast(scriptsFilesContent)
    // 创建一个数据源RDD
    val dataRDDTmp = sc.parallelize(List("hi", "hello", "how", "are", "you"), 2)
    // 将广播中的代码内容写到每一台机器上的本地文件中
    dataRDDTmp.foreachPartition(_ => {
      scriptsFilesB.value.foreach { case (filePath, content) =>
        val fileName = filePath.substring(filePath.lastIndexOf("/") + 1)
        val file = new File(s"/home/hadoop-twq/spark-course/pipe/${fileName}")
        if (!file.exists()) {
          val buffer = new BufferedWriter(new FileWriter(file))
          buffer.write(content)
          buffer.close()
        }
      }
    })
    // 对数据源rdd调用pipe以分布式的执行我们定义的python脚本
    val hdfsPipeRDD = dataRDDTmp.pipe(s"python /home/hadoop-twq/spark-course/pipe/echo_hdfs.py")

    hdfsPipeRDD.glom().collect()


    //我们不能用如下的方法来达到将hdfs上的文件分发到每一个executor上
    sc.addFile("hdfs://master:9999/users/hadoop-twq/pipe/echo_hdfs.py")
    SparkFiles.get("echo.py")
  }

  /**
    * persist测试
    * persist(StorageLevel)
    * cache()
    * unpersist
    */
  def test4(): Unit ={
    val paralRDD = sc.parallelize(Seq(1,23,5), 4)
    paralRDD.persist()// 将rdd的数据缓存到内存中
    paralRDD.cache()// == paralRDD.persist()
    paralRDD.getStorageLevel //获取缓存级别
    paralRDD.toDebugString //查看依赖链
    sc.setCheckpointDir("hdfs://xxx")
  }
}
