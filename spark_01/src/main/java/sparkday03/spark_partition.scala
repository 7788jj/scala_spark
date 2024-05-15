package sparkday03
/*
分区操函数在生产环境当中我们之前的wordcount代码不能使用在map函数与foreach函数当中采用分区函数来代替.企业要求。
对分区的增加与减少。
 */
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

object spark_partition {
  def main(args: Array[String]): Unit = {


    //TODO spark的程序入口为sprkContext,需要先创建一个sparkconf对象
    val conf: SparkConf = new SparkConf()
      //设置主程序入口
      .setAppName("spark_wordcount")
      //设置为本地模式并分配两个核
      .setMaster("local[1]")
    //TODO 创建一个sprkcontext对象并传入conf
    val sc: SparkContext = new SparkContext(conf)


    //todo 第一步 读取数据封装到RDD集合中
    val inputRDD: RDD[String] = sc.textFile("C:\\Users\\CYC11\\IdeaProjects\\scala_spark\\spark_01\\src\\data\\data.txt" , minPartitions = 2)
//    //在数据集较大的数据需要增加分区但是，增加分区可能会增加shullfe，慎用
//    inputRDD.repartition(3)
//    val partitions: Int = inputRDD.getNumPartitions
//    println(s"分区数为${partitions}")
//    //当入的分区域设置的分区不一样的时候可以减少分区,减少不会产生shuffle
//    inputRDD.coalesce(2)




    //todo 第二步 分析数据调用RDD的函数
    val resultRDD: RDD[(String, Int)] = inputRDD
      //去除读取的数据的空格与其他制表符
      .filter(line => line != null && line.length != 0)
      //对读取的数据进行分割
      .flatMap(line => line.split(" "))
      //将数据进行统计加一
      //TODO 在分区里面的数据比较大的时候不能使用map需要使用分区函数mappartitions，迭代器来调用方法
      .mapPartitions{iter => iter.map(word => (word, 1))}
      //按照word来分组聚合
      .reduceByKey((tmp, item) => tmp + item)



    //TODO 第三步 保存数据，将数据保存到存储系统
    //TODO 这个也是需要改变为foreachpartition。通过迭代器对象来调用方法然后处理数据迭代器为集合里面的数据
    //使用偏函数来写可以和好一点
    resultRDD.foreachPartition{
      iter =>
        //获取当前分区的id
        val partitionID : Int = TaskContext.getPartitionId()
        iter.foreach{
          iter => println(s"${partitionID} : ${iter}")
        }
    }
  }

}
