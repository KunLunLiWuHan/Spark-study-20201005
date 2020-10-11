package chapter02

import org.apache.spark.rdd.RDD

//class RddProgram.Search(query: String)  extends Serializable {
class Search(query: String) {
  //过滤出包含字符串的数据
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  //过滤出包含字符串的RDD
  def getMatch1(rdd: RDD[String]): RDD[String] = {
    rdd.filter(isMatch)
  }

  //过滤出包含字符串的RDD
  def getMatche2(rdd: RDD[String]): RDD[String] = {
    //    rdd.filter(x => x.contains(query))
    /**
     * 方法2：将类变量赋值给局部变量
     * （1）query_ 是一个Driver的局部变量。
     * （2）Driver需要向Executor端传递的是一个字符串query_，字符串本身就可以序列化。
     */
    val query_ : String = this.query
    rdd.filter(x => x.contains(query_))
  }
}
