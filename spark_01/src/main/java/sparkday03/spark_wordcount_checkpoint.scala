package sparkday03

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object spark_wordcount_checkpoint {
def main(args: Array[String]): Unit = {
  //设置hdfs客户端的使用人。权限设置


  //TODO spark的程序入口为sprkContext,需要先创建一个sparkconf对象
  val conf: SparkConf = new SparkConf()
  //设置主程序入口
    .setAppName("spark_wordcount")
    //设置为本地模式并分配两个核
    .setMaster("local[1]")
  //TODO 创建一个sprkcontext对象并传入conf
  val sc = new SparkContext(conf)

  //todo 设置检测点位置
  sc.setCheckpointDir("C:\\Users\\CYC11\\IdeaProjects\\scala_spark\\spark_01\\src\\data\\check")

  //todo 第一步 读取数据封装到RDD集合中
  val inputRDD: RDD[String] = sc.textFile("C:\\Users\\CYC11\\IdeaProjects\\scala_spark\\spark_01\\src\\data\\data.txt")
  //TODO 将RDD的数据保存到检察点当中，这个也是懒函数需要触发
    inputRDD.checkpoint()
  //TODO 触发检查点
  inputRDD.count()


  Thread.sleep(10000000)


  //TODO 应用结束关闭资源
  sc.stop()











}

}
