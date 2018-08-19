package com.my.spark.sql
import com.my.spark.util.Utils._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  */
object JoinApiTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]")
      .appName("DataPartitionTest").getOrCreate()
    import spark.implicits._

    val ordersDF = spark.read.json(s"${BASIC_PATH}/spark/dataset/orders.json")
    val usersDF = spark.read.json(s"${BASIC_PATH}/spark/dataset/users.json")
    val orderItemsDF = spark.read.json(s"${BASIC_PATH}/spark/dataset/order_items.json")
    //join操作
    ordersDF.join(usersDF, "userId").show
    ordersDF.join(usersDF, Seq("userId", "userName")).show()//关联两个相同字段
    //可以跟上连接类型，inner,outer, left_outer,right_outer, leftsemi,leftanti,
    //默认为inner
    ordersDF.join(usersDF, Seq("userId"), "right_outer").show

    //查询出users中的userId和userName在orders存在的users
    usersDF.join(ordersDF,Seq("userId", "userName"), "leftsemi").show()

    //查询出users中的userId和userName在orders不存在的users
    usersDF.join(ordersDF,Seq("userId", "userName"), "leftanti").show()

    //根据不同字段名来进行join
    ordersDF.join(orderItemsDF,ordersDF("id") === orderItemsDF("orderId")).show

  //joinwith，返回一个二元组类型的dataset
    //第一个元素类型为第一个dataset中的类型，第二个元素类型为第二个dataset中的类型
    val ordersDS = ordersDF.as[Order]
    val usersDS = usersDF.as[User]
    ordersDS.joinWith(usersDS, ordersDS("userId") === usersDS("orderId"), "outer")

    val joinWithDS:Dataset[(Row,Row)] = ordersDF.joinWith(orderItemsDF,ordersDF("id") === orderItemsDF("orderId"))
  joinWithDS.show()
//    +-----------------+--------------------+
//    |               _1|                  _2|
//    +-----------------+--------------------+
//    |  [1,80.0,1,Join]|[4,1,apple,1,20.0,1]|
//    |  [2,50.0,1,Join]| [5,2,book,2,10.0,1]|
//    |[3,200.0,2,Jeffy]|[1,3,cake,3,200.0,2]|
//    +-----------------+--------------------+

  }
}
