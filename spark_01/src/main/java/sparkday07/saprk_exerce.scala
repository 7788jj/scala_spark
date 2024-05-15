package sparkday07

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object saprk_exerce {
  def main(args: Array[String]): Unit = {
    //创建sparksession对象
    val spark: SparkSession = SparkSession.builder()
      .appName("saprk_exerce")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._


    //创建sparkcontext对象来读取数据
    val sc: SparkContext = spark.sparkContext
    val inputRDD: RDD[String] = sc.textFile("C:\\Users\\CYC11\\IdeaProjects\\scala_spark\\spark_01\\src\\data\\ml-100k\\u.data")

    //对RDD数据进行处理
    val outRDD: RDD[MovieRating] = inputRDD.mapPartitions{ iter =>
      iter.map{line =>
        val   Array(userID, itemID, reting, timestmp)= line.trim.split("\\t")
        MovieRating(userID, itemID, reting.toDouble, timestmp.toLong)
      }
    }

    //转为dataframe
    val frame: DataFrame = outRDD.toDF()
    frame.printSchema()
    frame.show(10, truncate = false)


  }

}
