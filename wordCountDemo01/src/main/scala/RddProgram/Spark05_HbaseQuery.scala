package RddProgram

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @time: 2020-10-05 17:15 
 * @author: likunlun 
 * @description: hbase操作
 */
object Spark05_HbaseQuery {
  def main(args: Array[String]): Unit = {
    //创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("hbaseRDD")

    //创建SparkContext
    val sc = new SparkContext(sparkConf)

    //构建HBase配置信息
    val conf: Configuration = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, "student") //设置表名 namespace:tableName

    //从HBase读取数据形成RDD
    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable], //rowkey的类型
      classOf[Result]// 查询结果集
    )

    //显示数据的条数
    val count: Long = hbaseRDD.count()
    println(count)

    //对hbaseRDD进行处理，遍历显示数据的
    hbaseRDD.foreach {
      case (rowkey, result) =>{
        val cells: Array[Cell] = result.rawCells()
        for(cell <- cells){
          println(Bytes.toString(CellUtil.cloneValue(cell)))
        }
      }
    }
    //关闭连接
    sc.stop()
  }
}
