package chapter02

import java.sql.{DriverManager, PreparedStatement}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @time: 2020-10-05 15:16 
 * @author: likunlun 
 * @description: mysql数据的插入
 */
object Spark04_MySQLInsert {
  def main(args: Array[String]): Unit = {
    //1.初始化配置信息及SparkContext
    val sparkConf: SparkConf = new SparkConf().setAppName("Opera_Mysql").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    //插入到数据库中的数据
    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("zs", 20), ("ls", 24)))
    //配置连接mysql的参数
    val driver = "com.mysql.cj.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/rdd?useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=UTC"
    val userName = "root"
    val passWd = "123"

    //一次性处理一个partition的数据，那么对于每个partition，只要创建一个数据库连接即可
    dataRDD.foreachPartition(
      datas => {
        Class.forName(driver)
        var connect = DriverManager.getConnection(url, userName, passWd)
        datas.foreach {
          case (username, age) => {
            var sql = "insert into user(name,age) values (?,?);"
            val statement: PreparedStatement = connect.prepareStatement(sql)
            statement.setString(1, username)
            statement.setInt(2, age)
            statement.executeUpdate()
            statement.close()
          }
        }
        connect.close()
      }
    )
    //释放资源
    sc.stop()
  }
}
