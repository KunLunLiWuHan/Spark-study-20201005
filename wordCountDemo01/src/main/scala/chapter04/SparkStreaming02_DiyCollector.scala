package chapter04

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @time: 2020-10-11 14:28 
 * @author: likunlun 
 * @description: 自定义采集器
 */
object SparkStreaming02_DiyCollector {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming-Collector")
    val streamingContext = new StreamingContext(conf, Seconds(5))
    //使用自定义的采集器，从端口处采集数据
    val receiverDStream: ReceiverInputDStream[String] = streamingContext.receiverStream(new MyReceiver("hadoop101", 9999))

    //将采集过来的数据进行分解（扁平化）
    val wordDStream: DStream[String] = receiverDStream.flatMap(line => {
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

/**
 * 声明采集器
 * 1、继承Receiver
 */
class MyReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  //这一行放在这里会报错：java.io.NotSerializableException: java.net.Socket
  //  private var socket: Socket = new Socket(host, port)
  private var socket: Socket = _

  def receive(): Unit = {
    socket = new Socket(host, port)
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, "UTF-8"))
    var line: String = null

    while ((line = reader.readLine()) != null) {
      //将采集的数据存储到采集器的内部进行转换
      if ("END".equals(line)) {
        return
      } else {
        this.store(line)
      }
    }
  }

  override def onStart(): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        receive()
      }
    }).start()
  }

  override def onStop(): Unit = {
    if (socket != null) {
      socket.close()
      socket = null
    }
  }
}