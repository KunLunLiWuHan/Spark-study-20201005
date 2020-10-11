package chapter04

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 累加性地统计单词个数信息
 */
object SparkStreaming04_UpdateState {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming-wordcount")
    //实时数据采集环境对象
    //采集周期：以指定的时间为周期采集实时数据
    val streamingContext = new StreamingContext(conf, Seconds(5))
    //保存数据的状态，需要设定检查点路径
    streamingContext.sparkContext.setCheckpointDir("cp")

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

    //将转换结构后的数据进行聚合处理
    val stateDStream: DStream[(String, Int)] = mapDStream.updateStateByKey {
      case (seq, buffer) => {
        var sum = buffer.getOrElse(0) + seq.sum
        Option(sum)
      }
    }
    //将结果打印出来
    stateDStream.print()

    //启动采集器
    streamingContext.start()
    //Driver等待采集器执行
    streamingContext.awaitTermination()
  }
}
