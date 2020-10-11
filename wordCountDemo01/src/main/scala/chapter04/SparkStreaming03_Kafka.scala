package chapter04

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @time: 2020-10-11 15:04 
 * @author: likunlun 
 * @description: 从kafka中采集数据并消费
 */
object SparkStreaming03_Kafka {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming-wordcount")
    //实时数据采集环境对象
    //采集周期：以指定的时间为周期采集实时数据
    val streamingContext = new StreamingContext(conf, Seconds(5))

    //从kafka中采集数据
    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      streamingContext,
      "hadoop101:2181",
      "A-groupId",
      Map("A-topic" -> 3),
      StorageLevel.MEMORY_AND_DISK_SER_2
    )
    //将采集过来的数据进行分解（扁平化）
    val wordDStream: DStream[String] = kafkaDStream.flatMap(
      t => {
        t._2.split(" ")
      }
    )
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
