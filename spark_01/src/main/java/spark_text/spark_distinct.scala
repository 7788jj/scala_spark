package spark_text

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.math.BigDecimal.RoundingMode

object spark_distinct {
  def main(args: Array[String]): Unit = {
    //1。创建SparkContext对象
    val conf = new SparkConf()
      .setAppName("spark_distinct")
      .setMaster("local[1]")
    val sc = new SparkContext(conf)

    //2.读取数据到RDD
    val file1 = "C:\\Users\\CYC11\\IdeaProjects\\scala_spark\\spark_01\\src\\data\\file1"
    val file2 = "C:\\Users\\CYC11\\IdeaProjects\\scala_spark\\spark_01\\src\\data\\file2"
    val file3 = "C:\\Users\\CYC11\\IdeaProjects\\scala_spark\\spark_01\\src\\data\\file3"

    val file1_value: RDD[String] = sc.textFile(file1)
    val file2_value: RDD[String] = sc.textFile(file2)
    val file3_value: RDD[String] = sc.textFile(file3)

    //3.合并文件
    val file4_value: RDD[String] = file1_value.union(file2_value).union(file3_value)
      .repartition(1)

    //4.对数据进行处理
    val grades = file4_value.map(line => {
      val fields = line.split(" ")
      (fields(0), BigDecimal(fields(1)).setScale(2, RoundingMode.HALF_UP))
    })

    val averageGrades = grades.aggregateByKey((BigDecimal(0), 0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    ).mapValues { case (total, count) => (total / count).setScale(2, RoundingMode.HALF_UP) }


    //5.关闭资源
    sc.stop()



  }

}
