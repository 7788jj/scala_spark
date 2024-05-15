package SparkDay01


import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStream_WordCount {
  def main(args: Array[String]): Unit = {

    // TODO: 1. 构建StreamingContext流式上下文实例对象
    val ssc: StreamingContext = {
      // a. 创建SparkConf对象，设置应用配置信息
      val sparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[3]")
      // b.创建流式上下文对象, 传递SparkConf对象，TODO: 时间间隔 -> 用于划分流式数据为很多批次Batch
      val context = new StreamingContext(sparkConf, Seconds(5))
      // c. 返回
      context
    }

    // TODO: 2. 读取数据到Dstream,设置接收器的存储级别
    val inputDStream: DStream[String] = ssc.socketTextStream("master", 9999, storageLevel = StorageLevel.MEMORY_AND_DISK)

    // TODO: 3. 对每个批次的数据就行词频统计
    val resultDStream: DStream[(String, Int)] = inputDStream
      //去除空值
      .filter(line => line != null && line.trim.length > 0)
      //分割字符
      .flatMap(iter => iter.split("\\s+"))
      //转换
      .mapPartitions { line =>
        line.map(word =>
          (word, 1)
        )
      }
      //聚合结果
      .reduceByKey(_ + _)
    resultDStream.print()
    //TODO：5.流试应用需要显示的启动
    ssc.start()
    // 流式应用启动以后，正常情况一直运行（接收数据、处理数据和输出数据），除非人为终止程序或者程序异常停止
    ssc.awaitTermination()
    // 关闭流式应用(参数一：是否关闭SparkContext，参数二：是否优雅的关闭）
    ssc.stop(stopSparkContext = true, stopGracefully = true)

  }
}
