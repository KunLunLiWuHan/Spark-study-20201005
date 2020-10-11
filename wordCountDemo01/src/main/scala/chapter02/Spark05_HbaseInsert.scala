package chapter02

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf


/**
 * @time: 2020-10-05 18:30 
 * @author: likunlun 
 * @description: ${description}
 */
object Spark05_HbaseInsert {
  def main(args: Array[String]): Unit = {
    //创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("hbaseRDD")

    //创建SparkContext
    val sc = new SparkContext(sparkConf)

    val dataRDD: RDD[(String, String)] = sc.makeRDD(List(("1003", "we"), ("1004", "mz")))

    //构建HBase配置信息
    val conf: Configuration = HBaseConfiguration.create()

    val putRdd: RDD[(ImmutableBytesWritable, Put)] = dataRDD.map {
      case (rowkey, name) => {
        val put = new Put(Bytes.toBytes(rowkey))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name))
        (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)
      }
    }

    //作业的配置
    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,"student")
    putRdd.saveAsHadoopDataset(jobConf)

    //关闭连接
    sc.stop()
  }
}
