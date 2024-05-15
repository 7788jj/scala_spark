package sparkday05

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object spark_share1 {
  def main(args: Array[String]): Unit = {
    //todo 创建sparkconf与sparkcontext对象
    val conf = new SparkConf()
      .setAppName("spark_share1")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)
    //todo 创建广播变量传递给Executor
    val list: Seq[String] =  Seq("@", "*", "%", "!")

    //TODO 将广播变量广播到Exector当中
    val broad: Broadcast[Seq[String]] = sc.broadcast(list)
    //TODO 定义累加器，当读取到广播变量的内容的时候加一
    val accm: LongAccumulator = sc.longAccumulator("count")


    //TODO 读数据到RDD当中
    val inputRDD: RDD[String] = sc.textFile("C:\\Users\\CYC11\\IdeaProjects\\scala_spark\\spark_01\\src\\data\\data.txt")
   val etlRDD: RDD[String] = inputRDD
      .flatMap{iter =>
        iter.split(" ")
      }
      .filter {iter=>
        //TODO 定义变量获取广播变量的值，并判断是否在广变量当中
        val listvalue: Seq[String] = broad.value
        val is = listvalue.contains(iter)
        if(is){
          accm.add(1L)
        }
        !is
      }
      //TODO 聚合函数先分区在集合
      val  resoultRDD: RDD[(String, Int)] = etlRDD
      .mapPartitions{iter =>
        iter.map(word =>
          (word , 1)
        )
      }
        .reduceByKey((tmp, iter) =>tmp + iter)
    //TODO 这个为懒函数需要触发对应结果以后在打印
   resoultRDD.foreach(println)

    println(s"计数器为${accm.value}")


    //计数器为LongAccumulator(id: 0, name: Some(sccm), value: 4)
   Thread.sleep(10000000)
    //TODO 释放资源
    sc.stop()


  }

}
