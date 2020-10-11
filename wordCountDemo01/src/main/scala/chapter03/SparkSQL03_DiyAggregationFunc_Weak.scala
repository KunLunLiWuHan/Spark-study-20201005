package chapter03

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @time: 2020-10-10 19:59 
 * @author: likunlun 
 * @description: 自定义弱聚合函数
 */
object SparkSQL03_DiyAggregationFunc_Weak {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSql01")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //自定义聚合函数
    //创建聚合函数
    val udf1 = new MyAgeAvgFunction()
    spark.udf.register("avgAge", udf1)
    //使用聚合函数
    val frame: DataFrame = spark.read.json("in/user.json")
    frame.createOrReplaceTempView("user")
    spark.sql("select avgAge(age), avgAge(grade) from user").show()
    spark.stop()
  }
}

/**
 * 声明用户自定义聚合函数
 * 1、继承UserDefinedAggregateFunction
 * 2、实现方法
 */
class MyAgeAvgFunction extends UserDefinedAggregateFunction {
  //函数输入的数据结构
  override def inputSchema: StructType = {
    /**
     * 1、age在这里只是一个变量，定义成xxx也没有关系。
     * 2、该输入参数只会在更新函数update()中的input参数用到。
     * 3、传入两个需要操作的数据进行求平均值，步骤一致。
     */
    //    new StructType().add("age", LongType)
    new StructType().add("age", LongType).add("grade", LongType)

  }

  //计算时的数据结构
  override def bufferSchema: StructType = {
    new StructType().add("num", LongType).add("count", LongType)
  }

  //函数返回的数据结构
  override def dataType: DataType = DoubleType

  //函数是否稳定，即相同输入是否有相同输出
  override def deterministic: Boolean = true

  //计算之前的缓冲区初始化
  /**
   * 1、分区内的初始化操作
   * 2、即给sum和count赋的初始值
   */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  //根据查询结果更新缓存区数据(分区)
  /**
   * 1、分区内的更新操作
   * 2、buffer是临时变量
   * 3、input是自定义函数调用时传入的值，一行一行的进行操作
   */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0) //sum += i
    print(buffer.getLong(0))
    buffer(1) = buffer.getLong(1) + 1 //count += 1
  }

  //将多个节点的缓冲区合并（分区合并）
  /**
   * 1、分区间的合并操作
   *
   * @param buffer1 假设有三个分区，其表示将前面的1，2，分区合并后的结果
   * @param buffer2 第三个分区的数据
   */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //sum
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    //count
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    println(1)
  }

  //最后进行计算后输出
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}