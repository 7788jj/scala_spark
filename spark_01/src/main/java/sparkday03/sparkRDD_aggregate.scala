package sparkday03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

import scala.collection.mutable.ListBuffer

/*
聚合函数aggregate，RDD的高级聚和函数（）

 */
object sparkRDD_aggregate {
  def main(args: Array[String]): Unit = {
    //创建sparkconf对象
    val conf = new SparkConf()
      .setAppName("parrallelize")
      .setMaster("local[2]")

    //创建sprkcontext对象
    val sc = new SparkContext(conf)

    val seq: Seq[Int] = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)

    //TODO 创建本地并行化集合数，并设置分区数
    val inputRDD: RDD[Int] = sc.parallelize(seq, numSlices = 2)

    //获取每一个分区的详细信息
    inputRDD.foreachPartition { iter =>
      println(s"p-${TaskContext.getPartitionId()}: ${iter.mkString(", ")}")
    }


    /*
    使用RDD当中的aggregate来就行聚合，有两个参数第一个为对参数进行初始化，
    后面的参数为两个函数，前面一个为分区内部处理函数定义中间变量的类型，与集合中间的每一个元素
    这里面变量类型为数组。后面的函数为分区间的数据处理逻辑，前面一个定义中间临时变量的类型，后面为集合的每一个分区
    为一个元素
     */

    val buffer: ListBuffer[Int] = inputRDD.aggregate(new ListBuffer[Int]())(
      (tmplist: ListBuffer[Int], iter) => {
        tmplist.append(iter)
        tmplist.sorted.takeRight(2)
      },

      (tmplist1: ListBuffer[Int], item: ListBuffer[Int]) => {
        tmplist1.appendAll(item)
        tmplist1.sorted.takeRight(2)
      }
    )
    //TODO 对结果就行打印
    println(buffer.toList.mkString(","))







  }

}
