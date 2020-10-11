package chapter02

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @time: 2020-10-05 19:06 
 * @author: likunlun 
 * @description: 自定义累加器
 */
object Spark06_ShareData_DIY {
  def main(args: Array[String]): Unit = {
    //创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Accumulator")
    //创建SparkContext
    val sc = new SparkContext(sparkConf)
    val dataRDD: RDD[String] = sc.makeRDD(List("hadoop", "spark", "hbase"), 2)

    //创建累加器对象
    val wordAccumulator = new WordAccumulator()
    //注册累加器
    sc.register(wordAccumulator)
    //执行
    dataRDD.foreach {
      case word => {
        wordAccumulator.add(word)
      }
    }
    println("累加器的值为：" + wordAccumulator.value)
  }
}

/**
 * 自定义累加器
 * 1、继承AccumulatorV2
 * 2、实现抽象方法
 */
class WordAccumulator extends AccumulatorV2[String, util.ArrayList[String]] {
  private val list = new util.ArrayList[String]()

  //当前的累加器是否为初始化状态
  override def isZero: Boolean = list.isEmpty

  //复制累加器对象
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
    new WordAccumulator()
  }

  //重置累加器对象
  override def reset(): Unit = list.clear()

  //向累加器中添加数据
  override def add(v: String): Unit = {
    if (v.contains("h")) {
      list.add(v)
    }
  }

  //合并
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    list.addAll(other.value)
  }

  //获取累加器的结果
  override def value: util.ArrayList[String] = list
}
