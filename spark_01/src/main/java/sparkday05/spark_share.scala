package sparkday05

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object spark_share {
  def main(args: Array[String]): Unit = {
    //todo 创建sparkconf与sparkcontext对象
    val conf = new SparkConf()
    conf.setAppName("spark_share")
      .setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    //todo 创建广播变量传递给Executor
    val list: List[String] = List("@", "*", "%", "!")
    //TODO 将广播变量广播到Exector当中
    val broad: Broadcast[List[String]] = sc.broadcast(list)
    //TODO 定义累加器，当读取到广播变量的内容的时候加一
    val value: LongAccumulator = sc.longAccumulator("sccm")

    //TODO 读数据到RDD当中
    val inputRDD: RDD[String] = sc.textFile("C:\\Users\\CYC11\\IdeaProjects\\scala_spark\\spark_01\\src\\data\\data.txt")
    val  resoultRDD = inputRDD
      .flatMap(iter =>
        iter.split(" ")
      )
      .filter{iter =>
        //TODO 定义变量获取广播变量的值，并判断是否在广变量当中
        val listvalue: List[String] = broad.value
        val islist = listvalue.contains(iter)
        //TODO 如果在你们计数器加一，并返回不在的数据
        if(islist){
          value.add(1L)
        }
        !islist
      }
      .mapPartitions{iter =>
      iter.map(str =>
        (str , 1)
      )
  }
      //TODO 聚合函数先分区在集合
      .reduceByKey((tmp ,iter) => tmp + iter)

    resoultRDD.foreachPartition{iter =>
      iter.foreach(println)
    }
    //TODO 这个为懒函数需要触发对应结果以后在打印
    println(s"计数器为${value.value}")

    //计数器为LongAccumulator(id: 0, name: Some(sccm), value: 4)
//    Thread.sleep(10000000)
    //TODO 释放资源
    sc.stop()

  }

}
