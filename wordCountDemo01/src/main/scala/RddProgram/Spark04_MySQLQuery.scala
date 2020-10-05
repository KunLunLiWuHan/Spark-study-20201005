package RddProgram

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @time: 2020-10-05 15:16 
 * @author: likunlun 
 * @description: mysql数据的查询
 */
object Spark04_MySQLQuery {
  def main(args: Array[String]): Unit = {
    //1.初始化配置信息及SparkContext
    val sparkConf: SparkConf = new SparkConf().setAppName("Opera_Mysql").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    //配置连接mysql的参数
    val driver = "com.mysql.cj.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/rdd?useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=UTC"
    val userName = "root"
    val passWd = "123"

    //创建JDBCRDD，方法数据库
    var sql = "select * from `user` where `id`>=? and `id` <= ?;"
    val jdbcRDD = new JdbcRDD(
      sc,
      () => {
        Class.forName(driver)
        DriverManager.getConnection(url, userName, passWd)
      },
      sql,
      1,
      3,
      1,
      r => (r.getInt(1), r.getString(2))
    )

    //打印最后结果
    println(jdbcRDD.count())
    jdbcRDD.foreach(println)

    //释放资源
    sc.stop()
  }
}
