package sparkday03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_wordcount {
def main(args: Array[String]): Unit = {
  //设置hdfs客户端的使用人。权限设置
//  System.setProperty("HADOOP_USER_NAME","root")

  //TODO spark的程序入口为sprkContext,需要先创建一个sparkconf对象
  val conf: SparkConf = new SparkConf()
  //设置主程序入口
    .setAppName("spark_wordcount")
    //设置为本地模式并分配两个核
    .setMaster("local[1]")
  //TODO 创建一个sprkcontext对象并传入conf
  val sc = new SparkContext(conf)

  //todo 第一步 读取数据封装到RDD集合中
  val inputRDD: RDD[String] = sc.textFile("C:\\Users\\CYC11\\IdeaProjects\\scala_spark\\spark_01\\src\\data\\data.txt")

  //todo 第二步 分析数据调用RDD的函数
    val resultRDD: RDD[(String, Int)] = inputRDD
      //去除读取的数据的空格与其他制表符
      .filter(line => line != null && line.length != 0)
    //对读取的数据进行分割
    .flatMap(line =>line.split(" "))
    //将数据进行统计加一
    .map(word =>(word, 1))
    //按照word来分组聚合
    .reduceByKey((tmp, item) => tmp + item)



  //TODO 第三步 保存数据，将数据保存到存储系统
  resultRDD.foreach(println)
  //将文件保存到hdfs上面，并一时间戳来命名
//  resultRDD.saveAsTextFile("/ddddd")


  //TODO 应用结束关闭资源
  sc.stop()











}

}
