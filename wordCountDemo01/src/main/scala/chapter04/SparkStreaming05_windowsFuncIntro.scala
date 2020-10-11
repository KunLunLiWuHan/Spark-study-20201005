package chapter04

/**
 * @time: 2020-10-11 20:47 
 * @author: likunlun 
 * @description: 窗口函数介绍
 */
object SparkStreaming05_windowsFuncIntro {
  def main(args: Array[String]): Unit = {
    val list = List(1, 2, 3, 4, 5, 6)
    /**
     * 滑动窗口函数
     * sliding有两个参数：
     * 参数1：尺寸
     * 参数2：步长
     */
    val iterator: Iterator[List[Int]] = list.sliding(3, 3)

    /**
     * 输出：
     * 1,2,3
     * 4,5,6
     */
    for (elem <- iterator) {
      println(elem.mkString(","))
    }
  }
}
