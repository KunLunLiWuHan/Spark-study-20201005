package RddProgram

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @time: 2020-10-05 19:29 
 * @author: likunlun 
 * @description: 广播变量
 */
object Spark07_BroadcastVar {
  def main(args: Array[String]): Unit = {
    //创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Accumulator")
    //创建SparkContext
    val sc = new SparkContext(sparkConf)
    val rdd1 = List((1, "a"), (2, "b"), (3, "c"))

    val list = List((1, 1), (2, 2), (3, 3))

    /**
     * 可以使用广播变量减少数据的传输，因为此时使用mapshuffle过程。
     * 1、构建广播变量
     */
    val broadcast: Broadcast[List[(Int, Int)]] = sc.broadcast(list)
    val result: List[(Int, (String, Any))] = rdd1.map {
      case (key, value) => {
        var v2: Any = null
        //2、使用广播变量
        for (t <- broadcast.value) {
          if (key == t._1) {
            v2 = t._2
          }
        }
        (key, (value, v2))
      }
    }

    /**
     * (1,(a,1))
     * (2,(b,2))
     * (3,(c,3))
     */
    result.foreach(println)
  }
}
