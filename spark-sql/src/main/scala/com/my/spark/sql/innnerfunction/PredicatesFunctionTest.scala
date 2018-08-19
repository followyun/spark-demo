package com.my.spark.sql.innnerfunction

import org.apache.spark.sql.SparkSession

/**
  * Created by tangweiqun on 2017/10/11.
  * 逻辑函数
  */
object PredicatesFunctionTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("PredicatesFunctionTest")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val dataSeq = Seq("Table,Blue,124", "Table,Red,223", "Chair,Blue,101", "Chair,Red,210")

    val df = spark.read.csv(dataSeq.toDS()).toDF("Item", "Color", "Quantity")

    df.createOrReplaceTempView("Inventory")

    spark.sql("select * from Inventory where Item = 'Table' and Color = 'Red'").show()
    /*
    +-----+-----+--------+
    | Item|Color|Quantity|
    +-----+-----+--------+
    |Table|  Red|     223|
    +-----+-----+--------+
     */

    spark.sql("select * from Inventory where Item = 'Table' or Color = 'Red'").show()
    /*
    | Item|Color|Quantity|
    +-----+-----+--------+
    |Table| Blue|     124|
    |Table|  Red|     223|
    |Chair|  Red|     210|
    +-----+-----+--------+
     */

    spark.sql("select * from Inventory where not(Item = 'Table' and Color = 'Red') ").show()
    /*
    +-----+-----+--------+
    | Item|Color|Quantity|
    +-----+-----+--------+
    |Table| Blue|     124|
    |Chair| Blue|     101|
    |Chair|  Red|     210|
    +-----+-----+--------+
     */

    spark.sql("select * from Inventory where Item in ('Table', 'Chair')").show()
    /*
    +-----+-----+--------+
    | Item|Color|Quantity|
    +-----+-----+--------+
    |Table| Blue|     124|
    |Table|  Red|     223|
    |Chair| Blue|     101|
    |Chair|  Red|     210|
    +-----+-----+--------+
     */


    spark.stop()
  }
}
