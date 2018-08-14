package com.my.mba.spark

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * scala版计算关联规则
  */
object AssociationRuleApp {
  val basePath = "data"
  val support = 0.00
  val confidence = 0.00
  val delimiter = ","

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AssociationRuleApp")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)
    val fileRDD = sc.textFile(s"${basePath}/test/test.csv", 1)
    val txnCount = fileRDD.count()

    // 获得所有候选项集
    val itemsCountRDD = fileRDD.flatMap(line => {
      val items = line.split(delimiter)
      (0 to items.size) flatMap items.combinations filter (xs => !xs.isEmpty)
    }).map((_, 1))
    val minSupportCount = support * txnCount
    //获得所有频繁项集
    val frequentRDD = itemsCountRDD.reduceByKey(_ + _).filter(_._2 >= minSupportCount)

    val subItemSetRDD = frequentRDD.flatMap {
      case (frequentItemSet, supportCount) =>
        val result = ListBuffer.empty[(List[String], (List[String], Int))]
        result += ((frequentItemSet, (Nil, supportCount)))
        //即把K作为K2，Tuple(null,V))作为V2
        val sublist =
          (0 to frequentItemSet.size - 1) flatMap frequentItemSet.combinations filter (xs => !xs.isEmpty) map((_, (frequentItemSet, supportCount)))
        result ++= sublist
        result.toList
    }
    val rules = subItemSetRDD.groupByKey()
    val associationRulesRDD = rules.flatMap { case (antecedent, allSuperItemSets) =>
      val antecedentSupportCount = allSuperItemSets.find(p => p._1 == Nil).get._2
      val superItemSets = allSuperItemSets.filter(p => p._1 != Nil).toList //将规则集合去掉空的
      if (superItemSets.isEmpty) Nil
      else {
        val result =
          for {
            (superItemSet, superItemSetSupportCount) <- superItemSets
            consequent = superItemSet diff antecedent
            ruleSupport = superItemSetSupportCount.toDouble / txnCount.toDouble
            confidence = superItemSetSupportCount.toDouble / antecedentSupportCount.toDouble if confidence >= minConfidence
          } yield (((antecedent, consequent, ruleSupport, confidence)))
        result
      }
    }
    val formatResult = associationRulesRDD.map(s => {
      (s._1.mkString("[", ",", "]"), s._2.mkString("[", ",", "]"), s._3, s._4)
    })

    FileSystem.get(sc.hadoopConfiguration).delete(new Path(output), true)
    formatResult.saveAsTextFile(output)
    sc.stop()
  }

}
