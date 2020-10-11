package chapter02

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @time: 2020-10-05 19:06 
 * @author: likunlun 
 * @description: 累加器
 */
object Spark06_ShareData {
  def main(args: Array[String]): Unit = {
    //创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Accumulator")
    //创建SparkContext
    val sc = new SparkContext(sparkConf)
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    //使用累加器来共享变量，累加数据，下面是创建累加器对象
    val accumulator: LongAccumulator = sc.longAccumulator

    dataRDD.foreach {
      case i => {
        //执行累加器的累加功能
        accumulator.add(i)
      }
    }
    //结果展示
    println("sum = " + accumulator.value)
  }
}
