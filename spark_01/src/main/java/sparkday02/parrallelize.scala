package sparkday02


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
创建本地并行化集合，与读取外部储存系统
 */
object parrallelize {
  def main(args: Array[String]): Unit = {
    //创建sparkconf对象
    val conf = new SparkConf()
      .setAppName("parrallelize")
      .setMaster("local[2]")

    //创建sprkcontext对象
      val sc = new SparkContext(conf)


    val seq = Seq(1, 2, 4, 5, 6)
    //本地数据需要传入集合，与并行度,RDD的创建
    //读取外部文件系统的方法,也可以制定RDD的最小分区数
    val inputRDD1: RDD[String] = sc.textFile("C:\\Users\\CYC11\\IdeaProjects\\scala_spark\\spark_01\\src\\data\\data.txt", minPartitions = 2)
    val inputRDD2 = sc.parallelize(seq, numSlices = 2)
    //打印结果
    inputRDD2.foreach(item => println(item))







     //关闭资源
    sc.stop()
  }





}
