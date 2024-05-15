package sparkday09

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object spark_sqletl_movie {
  def main(args: Array[String]): Unit = {
    // 构建SparkSession实例对象
    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .getOrCreate()
    // 导入隐式转换
    import spark.implicits._
    // 1. 读取电影评分数据，从本地文件系统读取
    val rawRatingsDS: Dataset[String] = spark.read.textFile("datas/ml-1m/ratings.dat")
    // 2. 转换数据
    val ratingsDF: DataFrame = rawRatingsDS
    // 过滤数据

      .filter(line => null != line && line.trim.split("::").length == 4)
      // 提取转换数据
      .mapPartitions { iter =>
        iter.map { line =>
          // 按照分割符分割，拆箱到变量中
          val Array(userId, movieId, rating, timestamp) = line.trim.split("::")
          // 返回四元组
          (userId, movieId, rating.toDouble, timestamp.toLong)
        }
      }
      // 指定列名添加Schema
      .toDF("userId", "movieId", "rating", "timestamp")
    /*
    root
    |-- userId: string (nullable = true)
    |-- movieId: string (nullable = true)
    |-- rating: double (nullable = false)
    |-- timestamp: long (nullable = false)
    */
    //ratingsDF.printSchema()
    /*
    +------+-------+------+---------+
    |userId|movieId|rating|timestamp|
    +------+-------+------+---------+
    | 1| 1193| 5.0|978300760|
    | 1| 661| 3.0|978302109|
    | 1| 594| 4.0|978302268|
    | 1| 919| 4.0|978301368|
    +------+-------+------+---------+
    */


    // TODO： 基于SQL方式分析
    // 第一步、注册DataFrame为临时视图
    ratingsDF.createOrReplaceTempView("view_temp_ratings")
    // 第二步、编写SQL
    val top10MovieDF: DataFrame = spark.sql(
      """
        |SELECT
        | movieId, ROUND(AVG(rating), 2) AS avg_rating, COUNT(movieId) AS cnt_rating
        |FROM
        | view_temp_ratings
        |GROUP BY
        | movieId
        |HAVING
        | cnt_rating > 2000
        |ORDER BY
        | avg_rating DESC, cnt_rating DESC
        |LIMIT
        | 10
    """.stripMargin)
    //top10MovieDF.printSchema()
    top10MovieDF.show(10, truncate = false)
  }

}
