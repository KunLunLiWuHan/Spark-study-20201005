package chapter02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

/**
 * @time: 2020-10-05 15:16 
 * @author: likunlun 
 * @description: ${description}
 */
object Spark03_Json {
  def main(args: Array[String]): Unit = {
    //1.初始化配置信息及SparkContext
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    //2.创建一个RDD
    val json: RDD[String] = sc.textFile("in/user.json")
    //解析json数据
    val result: RDD[Option[Any]] = json.map(JSON.parseFull)
    result.foreach(println)

    //释放资源
    sc.stop()
  }
}
