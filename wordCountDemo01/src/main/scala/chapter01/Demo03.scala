package chapter01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo03 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("My Master")
    val sc = new SparkContext(conf)
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)
    /**
     * 需求：创建一个RDD,使每个元素所在分区，形成一个
     * 元组进而组成一个新的RDD （数据，分区）
     * 1、使用模式匹配
     * case (num,datas) 中 num 是分区号，datas分区中的数据集合
     * 即传入的数据
     */
    val value: RDD[(Int, String)] = listRDD.mapPartitionsWithIndex {
      case (num, datas) => {
        //先考虑语法，后考虑实现逻辑。数据原样数据
        //datas
        // _表示datas中的每一条数据（1 -》 10），后面是分区号
        datas.map((_, "分区号：" + num))
      }
    }
    value.collect().foreach(println)
  }
}
