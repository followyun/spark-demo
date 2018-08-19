package com.my.spark.sql.innnerfunction

import org.apache.spark.sql.SparkSession

/**
  * Created by tangweiqun on 2017/10/11.
  * 分组函数
  */
object GroupingSetsFunctionTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("GroupingSetsFunctionTest")
      .master("local")
      .getOrCreate()

    //参考： https://msdn.microsoft.com/zh-cn/library/ms175939(SQL.90).aspx
    //  https://msdn.microsoft.com/zh-cn/library/ms189305(v=sql.90).aspx
    //  https://msdn.microsoft.com/zh-cn/library/bb510624(v=sql.105).aspx

    import spark.implicits._

    val dataSeq = Seq("Table,Blue,124", "Table,Red,223", "Chair,Blue,101", "Chair,Red,210")

    val df = spark.read.csv(dataSeq.toDS()).toDF("Item", "Color", "Quantity")

    df.createOrReplaceTempView("Inventory")

    //cube
    spark.sql(
      """
        |SELECT Item, Color, SUM(Quantity) AS QtySum
        |FROM Inventory
        |GROUP BY Item, Color WITH CUBE
      """.stripMargin).show()
    /*
    +-----+-----+------+
    | Item|Color|QtySum|
    +-----+-----+------+
    |Table| null| 347.0|
    |Table| Blue| 124.0|
    | null| null| 658.0|
    | null| Blue| 225.0|
    |Chair|  Red| 210.0|
    |Chair| null| 311.0|
    |Chair| Blue| 101.0|
    |Table|  Red| 223.0|
    | null|  Red| 433.0|
    +-----+-----+------+
     */

    //GROUPING + cube
    spark.sql(
      """
        SELECT CASE WHEN (GROUPING(Item) = 1) THEN 'ALL'
       |            ELSE nvl(Item, 'UNKNOWN')
       |       END AS Item,
       |       CASE WHEN (GROUPING(Color) = 1) THEN 'ALL'
       |            ELSE nvl(Color, 'UNKNOWN')
       |       END AS Color,
       |       SUM(Quantity) AS QtySum
       |FROM Inventory
       |GROUP BY Item, Color WITH CUBE
      """.stripMargin).show()
    /*
    +-----+-----+------+
    | Item|Color|QtySum|
    +-----+-----+------+
    |Table|  ALL| 347.0|
    |Table| Blue| 124.0|
    |  ALL|  ALL| 658.0|
    |  ALL| Blue| 225.0|
    |Chair|  Red| 210.0|
    |Chair|  ALL| 311.0|
    |Chair| Blue| 101.0|
    |Table|  Red| 223.0|
    |  ALL|  Red| 433.0|
    +-----+-----+------+
     */

    //GROUPING + ROLLUP
    spark.sql(
      """
        SELECT CASE WHEN (GROUPING(Item) = 1) THEN 'ALL'
       |            ELSE nvl(Item, 'UNKNOWN')
       |       END AS Item,
       |       CASE WHEN (GROUPING(Color) = 1) THEN 'ALL'
       |            ELSE nvl(Color, 'UNKNOWN')
       |       END AS Color,
       |       SUM(Quantity) AS QtySum
       |FROM Inventory
       |GROUP BY Item, Color WITH ROLLUP
      """.stripMargin).show()
    /*
    +-----+-----+------+
    | Item|Color|QtySum|
    +-----+-----+------+
    |Table|  ALL| 347.0|
    |Table| Blue| 124.0|
    |  ALL|  ALL| 658.0|
    |Chair|  Red| 210.0|
    |Chair|  ALL| 311.0|
    |Chair| Blue| 101.0|
    |Table|  Red| 223.0|
    +-----+-----+------+
     */

    //GROUPING + ROLLUP + GROUPING_ID
    spark.sql(
      """
        SELECT CASE WHEN (GROUPING(Item) = 1) THEN 'ALL'
       |            ELSE nvl(Item, 'UNKNOWN')
       |       END AS Item,
       |       CASE WHEN (GROUPING(Color) = 1) THEN 'ALL'
       |            ELSE nvl(Color, 'UNKNOWN')
       |       END AS Color,
       |       GROUPING_ID(Item, Color) AS GroupingId,
       |       SUM(Quantity) AS QtySum
       |FROM Inventory
       |GROUP BY Item, Color WITH ROLLUP
      """.stripMargin).show()
    /*
    +-----+-----+----------+------+
    | Item|Color|GroupingId|QtySum|
    +-----+-----+----------+------+
    |Table|  ALL|         1| 347.0|
    |Table| Blue|         0| 124.0|
    |  ALL|  ALL|         3| 658.0|
    |Chair|  Red|         0| 210.0|
    |Chair|  ALL|         1| 311.0|
    |Chair| Blue|         0| 101.0|
    |Table|  Red|         0| 223.0|
    +-----+-----+----------+------+
     */
    spark.stop()
  }
}
