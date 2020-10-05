package chapter01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo04 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("My Master")
    val sc = new SparkContext(conf)
    //    val listRDD: RDD[List[Int]] = sc.makeRDD(Array(List(1, 2), List(3, 4)))
    //    //把 List(1, 2)，List(3, 4)两个元素，当成datas,进行拆解后返回1，2，3，4
    //    val value: RDD[Int] = listRDD.flatMap(datas => datas)
    //    value.collect().foreach(println)

    //    val ListRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 2)
    //    val glomRDD: RDD[Array[Int]] = ListRDD.glom()
    //    //glom：将一个分区形成一个数组，形成新的类型RDD[Array[T]]
    //    glomRDD.collect().foreach(array => {
    //      /**
    //       * 1,2
    //       * 3,4,5
    //       */
    //      println(array.mkString(","))
    //    })
    val ListRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))
    //(Int, Iterable[Int])  :前者i % 2的值，后者数据的结果
    //    val value: RDD[(Int, Iterable[Int])] = ListRDD.groupBy(i => i % 2)
    val value: RDD[Int] = ListRDD.filter(x => (x % 2 == 0))
    //形成一个元组（k,v），k表示分组的key,v表示分组后的数据集合
    //(0,CompactBuffer(2, 4))
    //(1,CompactBuffer(1, 3, 5))
    value.collect().foreach(println)
  }
}
