package chapter01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CreateRDDDemo01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("My Master")
    val sc = new SparkContext(conf)

    //创建RDD
    //1、从集合中创建 makeRDD (Int类型)
    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    listRDD.collect().foreach(println)
    val arrayRDD: RDD[Int] = sc.parallelize(List(1, 2, 3, 4))
    arrayRDD.collect().foreach(println)

    /**
     * 2、从外部存储中创建
     * 默认情况下，可以读取项目路径，也可以读取其他路径：HDFS
     * 默认从文件中读取的数据都是字符串类型
     */
      println("-----------------------------------------")
    val value: RDD[String] = sc.textFile("in")
//    val lines: RDD[String] = sc.textFile("hdfs://node-6:9000/user/zookeeper/in01/wc01.txt")
    value.collect().foreach(println)
  }
}
