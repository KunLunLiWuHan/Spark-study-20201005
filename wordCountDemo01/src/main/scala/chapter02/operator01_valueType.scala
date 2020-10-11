package chapter02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @time: 2020-10-02 19:52
 * @author: likunlun
 * @description: 算子测试
 */
object operator01_valueType {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Operator").setMaster("local[4]")
    val sc = new SparkContext(conf)

    /**
     * 1、从指定的数据集合中进行抽样处理。
     * 生成数据，按照指定规则进行过滤。
     * 2、fraction ：抽出多少，这是一个double类型的参数,0-1之间，eg:0.3表示抽出30%
     */
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)
    //false表示无放回的抽样
    val sampleRDD: RDD[Int] = listRDD.sample(false, 1, 1)
    sampleRDD.collect().foreach(println)
  }
}
