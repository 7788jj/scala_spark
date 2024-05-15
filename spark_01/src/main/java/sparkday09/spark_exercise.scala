package sparkday09

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.{DataFrame, SparkSession}

object spark_exercise {
  def main(args: Array[String]): Unit = {
    //TODO 创建sparkSession对象
    val spark: SparkSession = SparkSession.builder()
      .appName("spark_exercise")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._


    //TODO 读取数据到RDD里面
    val inputRDD: RDD[String] = spark.sparkContext.textFile("C:\\Users\\CYC11\\IdeaProjects\\scala_spark\\spark_01\\src\\data\\ml-1m\\ratings.dat", 2)

    //todo 将读取数据进行拆包与封装
    val frame: DataFrame = inputRDD.mapPartitions { line =>
      line.map { iter =>
        val Array(userId, movieId, rating, timestamp) = iter.split("::")
        (userId, movieId, rating, timestamp)
      }
    }.toDF("userId", "movieId", "rating", "timestamp")

    //将数据进行缓存
    frame.persist()
    //TODO 使用sql来就行分析数据
    //1.先将结果注册为临时视图
    frame.createOrReplaceTempView("tmp_rating")
    //2.写sql语句分析
    /*
    对电影评分数据进行统计分析，获取Top10电影（电影评分平均值最高，并且每个电影被评分
    的次数大于2000)。
     */
    val frame1: DataFrame = spark.sql(
      """
        |select movieId, round(avg(rating), 2) as avg_rating, count(movieId) as avg_count
        |from tmp_rating
        |group by movieId
        |having avg_count >=2000
        |order by avg_rating desc , avg_count desc
        |limit 10
        |""".stripMargin)

    frame1.show(10, truncate = false)


    //todo 使用DSL编程来实现
    import  org.apache.spark.sql.functions._

    frame.select($"movieId", $"rating")
      .groupBy($"movieId")
      .agg(
        round(avg($"rating"), 2).as("avg_rating"),
        count($"movieId").as("avg_count")
      )
      .filter($"avg_count" >= 2000)
      .orderBy($"avg_rating".desc, $"avg_count".desc)
      .limit(10)
      .show(10, truncate = false)


    //TODO 保存数据到mysql，csv
    frame.write.format("com.mysql.cj.jdbc.Driver")
     .option("url", "jdbc:mysql://master:3306")
     .option("user", "root")
     .option("password", "123456")
    //保存到csv
    frame.write.csv("C:\\Users\\CYC11\\IdeaProjects\\scala_spark\\spark_01\\src\\data\\ml-1m\\ratings.csv")

    //todo 释放缓存， 关闭资源
    frame.unpersist()
    spark.stop()
  }

}
