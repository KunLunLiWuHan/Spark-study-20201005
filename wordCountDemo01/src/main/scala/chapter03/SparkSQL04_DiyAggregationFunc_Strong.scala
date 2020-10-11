package chapter03

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

object SparkSQL04_DiyAggregationFunc_Strong {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSql01")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    //创建聚合对象
    val udf2 = new MyAgeAvgFunction2
    //将聚合函数转换为查询列
    val avgCol: TypedColumn[UserBean, Double] = udf2.toColumn.name("avgAge")
    val frame: DataFrame = spark.read.json("in/user2.json")
    val userDS: Dataset[UserBean] = frame.as[UserBean]
    //应用函数
    userDS.select(avgCol).show()

    spark.stop()
  }
}

case class UserBean(name: String, age: Long)

case class AvgBuffer(var sum: Long, var count: Long)

/**
 * UserBean:输入参数类型
 * AvgBuffer:缓冲区
 * Double：输出类型
 */
class MyAgeAvgFunction2 extends Aggregator[UserBean, AvgBuffer, Double] {
  //初始化
  override def zero: AvgBuffer = {
    AvgBuffer(0, 0)
  }

  /**
   * 分区内的聚合数据
   *
   * @param b
   * @param a
   * @return
   */
  override def reduce(b: AvgBuffer, a: UserBean): AvgBuffer = {
    b.sum = b.sum + a.age
    b.count = b.count + 1
    b
  }

  /**
   * 分区间的合并数据
   *
   * @param b1
   * @param b2
   * @return
   */
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }

  //完成计算
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble / reduction.count
  }

  //转码对象，自定义类
  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}