package chapter04

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @time: 2020-10-11 14:05 
 * @author: likunlun 
 * @description: 统计单词个数
 */
object SparkStreaming01_WordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming-wordcount")
    //实时数据采集环境对象
    //采集周期：以指定的时间为周期采集实时数据
    val streamingContext = new StreamingContext(conf, Seconds(3))
    //从指定的端口中采集数据
    val socketLineDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("hadoop101", 9999)

    //将采集过来的数据进行分解（扁平化）
    val wordDStream: DStream[String] = socketLineDStream.flatMap(line => {
      line.split(" ")
    })
    //将数据进行结构的转换方便分析
    val mapDStream: DStream[(String, Int)] = wordDStream.map((_, 1))
    //将转换结构的数据进行聚合处理
    val word2SumDStream: DStream[(String, Int)] = mapDStream.reduceByKey(_ + _)

    //将结果打印出来
    word2SumDStream.print()

    //启动采集器
    streamingContext.start()
    //Driver等待采集器执行
    streamingContext.awaitTermination()
  }
}
