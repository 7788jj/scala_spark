package sparkday09

import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import java.util.Properties


object spark_dsletl_movie {
  def main(args: Array[String]): Unit = {
    //TODO 创建sparkSesion对象
    val spark = SparkSession.builder()
      .appName("spark_dsletl_movie")
      .master("local[2]")
      .getOrCreate()
    //TODO 导入隐式转换类
    import spark.implicits._
    //TODO 导入dsl编程的需要函数
    import org.apache.spark.sql.functions._

    //TODO 读取数据到RDD当中
    val inputRDD = spark.sparkContext.textFile("C:\\Users\\CYC11\\IdeaProjects\\scala_spark\\spark_01\\src\\data\\ml-1m\\ratings.dat", minPartitions = 2)

    //TODO 将RDD转换为DataFrame
    val frame: DataFrame = inputRDD
      .mapPartitions { iter =>
        iter.map { line =>
          //TODO 拆箱搭配元组里面
          val Array(userID, movieID, rating, timestamp) = line.split("::")
          //TODO 返回元组
          (userID, movieID, rating, timestamp)
        }
      }.toDF("userId", "movieId", "rating", "timestamp")

    //TODO 将数据进行分析
    /*
    对电影评分数据进行统计分析，获取Top10电影（电影评分平均值最高，并且每个电影被评分
    的次数大于2000)。
     */
    val value: Dataset[Row] = frame.select($"movieID", $"rating")
  //TODO 先分组在聚合
      .groupBy($"movieId")
      .agg(
        round(avg($"rating"), 2).as("avg_rating"),
        count($"movieID").as("cnt_rating")
        )
  //TODO 在过滤与排序
      .filter($"cnt_rating" >= 2000)
      .orderBy($"avg_rating".desc, $"cnt_rating".desc)
      .limit(10)

    //TODO 输出结果
     value.show(10, truncate = false)
    //TODO 如果数据较小，缓存数据
//    value.cache()

    //todo 保存数据到mysql与csv文件

//    value.write
//      .mode(SaveMode.Overwrite)
//      .option("driver", "com.mysql.cj.jdbc.Driver")
//      .option("user", "root")
//      .option("password", "123456")
//      .jdbc(
//        "jdbc:mysql://master:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode = true",
//    "cyc.tb_top10_movies",
//    new Properties()
//    )
//    //TODO 保存数据到csv文件
//    value.write
//      .mode("Overwrite")
//      .csv("C:\\Users\\CYC11\\IdeaProjects\\scala_spark\\spark_01\\src\\data\\ml-1m\\top10_movies.csv")
//
////线程睡觉
//    Thread.sleep(1000000)
//    //TODO 关闭sparkSession
//    spark.stop()
  }
}
