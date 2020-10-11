package chapter03

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @time: 2020-10-10 18:47 
 * @author: likunlun 
 * @description: RDD、DataFrame、DataSet之间的相互转换
 */
object Spark01_sqlTransform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSql01")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //创建RDD
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zs", 10), (2, "ls", 20)))

    /**
     * 转换之前，需要引入隐式转换
     * 这里的spark不是包名的含义，是SparkSession对象的名字
     */
    import spark.implicits._

    //1、Rdd转换成DF
    val df: DataFrame = rdd.toDF("id", "name", "age")

    //2、DF转换为DS
    val ds: Dataset[User] = df.as[User]
    //3、DS转换为DF
    val df1: DataFrame = ds.toDF()
    //4、DF转换为rdd
    val rdd1: RDD[Row] = df1.rdd
    rdd1.foreach(row => {
      //通过索引获取数据
      println(row.getString(1))
    })

    //5、RDD和DataSet之间的相互转换
    val UserRDD: RDD[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }
    val userDS: Dataset[User] = UserRDD.toDS()
    val rdd2: RDD[User] = userDS.rdd
    rdd2.foreach(println)

    spark.stop()
  }
}

case class User(id: Int, name: String, age: Int)