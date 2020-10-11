package chapter01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 李昆伦
 * @time 2020.07.30
 * @des 使用开发工具完成spark的开发
 *      local模型下使用
 */
object ScalaWordCount {
  def main(args: Array[String]): Unit = {
    /**
     * 部署spark计算框架的运行环境 yarn或者本地
     * setAppName:设置Spark应用的名称，会在集群的web界面里显示出来。
     * setMaster("local[4]")：以四个核在本地运行。
     */
    val conf = new SparkConf().setAppName("ScalaWordCount").setMaster("local[4]")
    //创建spark执行的入口
    val sc = new SparkContext(conf)

    //sc ---> org.apache.spark.SparkContext@4d41ba0f
    println("sc ---> " + sc)
    //指定以后从哪里读取数据创建RDD（弹性分布式数据集）
    //sc.textFile(args(0)).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).sortBy(_._2, false).saveAsTextFile(args(1))

    //    val lines: RDD[String] = sc.textFile(args(0))
    //确实可以到HDFS集群中下拉文件。
    //    val lines: RDD[String] = sc.textFile("hdfs://node-6:9000/user/zookeeper/in01/wc01.txt")
    //读取本目录下的文件(一行一行读出来)
    val lines: RDD[String] = sc.textFile("in/1.txt")

    /**
     * 1、切分压平
     * 数据：
     * hello hello
     * xiaolun
     * world
     */
    val words: RDD[String] = lines.flatMap(_.split(" "))
    println("words -->" + words.collect())
    //将单词和一组合
    val wordAndOne: RDD[(String, Int)] = words.map((_, 1))
    println("wordAndOne ---> " + wordAndOne.collect())
    /**
     * 按key进行聚合
     * (作用就是对相同key的数据进行处理，最终每个key只保留一条记录)
     * reduceByKey会寻找相同key的数据，当找到这样的两条记录时会对其value(分别记为x,y)做(x,y) => x+y的处理
     */
    val reduced: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)
    println("reduced ---> " + reduced)
    //排序
    //    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2, false)
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2, false)
    //将结果保存到HDFS中
    //        sorted.saveAsTextFile(args(1))
    //这种情况下，会创建文件，但是是在user=34938情况下创建的，不会上传到集群。
    //    sorted.saveAsTextFile("/user/zookeeper/out01")
    //由于集群用户为34938，所以会有报错Permission denied
    //    sorted.saveAsTextFile("hdfs://node-6:9000/user/zookeeper/out01")
    //    sorted.collect().foreach(println) //打印出来
    //    val result = sorted.collect()
    val res: Array[(String, Int)] = sorted.collect()
    res.foreach(println)
    sc.stop() //释放资源
  }
}
