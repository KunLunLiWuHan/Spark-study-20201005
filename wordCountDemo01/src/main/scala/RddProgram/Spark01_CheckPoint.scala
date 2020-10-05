package RddProgram

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @time: 2020-10-05 15:15 
 * @author: likunlun 
 * @description: Rdd中的checkpoint
 */
object Spark01_CheckPoint {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("checkPoint")
    //创建Spark的上下文对象
    val sc: SparkContext = new SparkContext(conf)
    //设定检查点的保存目录
    sc.setCheckpointDir("cp")

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val mapRDD: RDD[(Int, Int)] = rdd.map((_, 1))
    val reduceRDD: RDD[(Int, Int)] = mapRDD.reduceByKey(_ + _)
    reduceRDD.checkpoint()

    reduceRDD.foreach(println)
    println(reduceRDD.toDebugString)
  }
}
