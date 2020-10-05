package chapter01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo02 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("My Master")
    val sc = new SparkContext(conf)

    //    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
    //    listRDD.saveAsTextFile("output")
    //读取文件时，传递的分区参数为最小分区数，但是不一定是这个分区数，取决于hadoop读取文件时的分片规则
    //    val lines: RDD[String] = sc.textFile("in/2.txt")
    //    lines.saveAsTextFile("output2")

    val lsitRDD: RDD[Int] = sc.makeRDD(1 to 10)
    /**
     * 实现lsitRDD数据扩大2倍
     * 1、lsitRDD.mapPartitions(datas => datas) 返回1 - 10
     * 因为只有两个分区，所以(datas => datas)直走两次。
     * 2、但是我们想要每个数据乘以2后返回，所以对于每个分区中的数据需要遍历
     * 3、mapPartitions和map实现功能一致，但效率优于map，将一个分区中的数据，全部传给一个Executor，
     * 减少了数据传输交互次数
     * 4、缺点：可能会出现内存溢出情况。
     */
    //    val value: RDD[Int] = lsitRDD.mapPartitions(datas => datas)
    val value: RDD[Int] = lsitRDD.mapPartitions(datas => {
      //datas 是 可迭代的集合。
      //下面是一个整体发给Executor,进行计算
      datas.map(data => data*2)
    })
    value.collect().foreach(println)
  }
}
