package chapter03

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @time: 2020-10-10 18:47 
 * @author: likunlun 
 * @description: 创建DataFrame读取数据
 */
object Spark01_sqlCreateDataFrame {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSql01")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //读取数据
    val frame: DataFrame = spark.read.json("in/user.json")
    //展示数据
//    frame.show()

    //将DataFrame展示成一张表
    frame.createOrReplaceTempView("user")
    //采用sql的方式访问数据
    spark.sql("select * from user").show()

    /**
     * +---+----+
     * |age|name|
     * +---+----+
     * | 20|   a|
     * | 24|   b|
     * | 28|   c|
     * +---+----+
     */
    spark.stop()
  }
}
