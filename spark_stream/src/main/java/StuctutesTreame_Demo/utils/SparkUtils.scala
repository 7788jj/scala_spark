package StuctutesTreame_Demo.utils

import StuctutesTreame_Demo.conf.ApplicationConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkUtils {
//创建SparkSession对象
  def createSparkSession(clazz : Class[_]) :SparkSession ={
    // 1.创建SparkConfig对象
    val sparkConf = new SparkConf()
      .setAppName(clazz.getSimpleName.stripSuffix("$"))
      .set("spark.debug.maxToStringFields", "2000")
      .set("spark.sql.debug.maxToStringFields", "2000")

    // 2.判断是否为本地模式
    if (ApplicationConfig.APP_LOCAL_MODE){
      sparkConf
        .setMaster(ApplicationConfig.APP_SPARK_MASTER)
        .set("spark.sql.shuffle.partitions","3")
    }
    // 3.创建sparkSession对象
    val session: SparkSession = SparkSession.builder()
      //传递配置
      .config(sparkConf)
      .getOrCreate()
    session
  }

  def createStreamingContext(clazz: Class[_], batchInterval: Int): StreamingContext = {
    // 构建对象实例
    val context: StreamingContext = StreamingContext.getActiveOrCreate(
      () => {
        // 1. 构建SparkConf对象
        val sparkConf: SparkConf = new SparkConf()
          .setAppName(clazz.getSimpleName.stripSuffix("$"))
          .set("spark.debug.maxToStringFields", "2000")
          .set("spark.sql.debug.maxToStringFields", "2000")
          .set("spark.streaming.stopGracefullyOnShutdown", "true")

        // 2. 判断应用是否本地模式运行，如果是设置值
        if (ApplicationConfig.APP_LOCAL_MODE) {
          sparkConf
            .setMaster(ApplicationConfig.APP_SPARK_MASTER)
            // 设置每批次消费数据最大数据量，生成环境使用命令行设置
            .set("spark.streaming.kafka.maxRatePerPartition", "10000")
        }
        // 3. 创建StreamingContext对象
        new StreamingContext(sparkConf, Seconds(batchInterval))
      }
    )
    context // 返回对象
  }

}
