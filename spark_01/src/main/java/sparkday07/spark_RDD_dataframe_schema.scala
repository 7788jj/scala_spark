import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
/**
 * 自定义Schema方式转换RDD为DataFrame
 */
object spark_RDD_dataframe_schema {
  def main(args: Array[String]): Unit = {
    // 构建SparkSession实例对象
    val spark: SparkSession = SparkSession
      .builder() // 使用建造者模式构建对象
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[3]")
      .getOrCreate()
    import spark.implicits._
    // 读取电影评分数据u.data, 每行数据有四个字段，使用制表符分割
    // user id | item id | rating | timestamp.
    val ratingsRDD: RDD[String] = spark
      .sparkContext.textFile("C:\\Users\\CYC11\\IdeaProjects\\scala_spark\\spark_01\\src\\data\\ml-100k\\u.data", minPartitions = 2)

    // a. RDD[Row]
    val rowsRDD: RDD[Row] = ratingsRDD.mapPartitions{ iter =>
      iter.map{line =>
        // 拆箱操作, Python中常用
        val Array(userId, itemId, rating, timestamp) = line.trim.split("\t")
        // 返回Row实例对象
        Row(userId, itemId, rating.toDouble, timestamp.toLong)
      }
    }
    // b. schema
    val rowSchema: StructType = StructType(
      Array(
        StructField("userId", StringType, nullable = true),
        StructField("itemId", StringType, nullable = true),
        StructField("rating", DoubleType, nullable = true),
        StructField("timestamp", LongType, nullable = true)
      )
    )
    // c. 应用函数createDataFrame
    val ratingDF: DataFrame = spark.createDataFrame(rowsRDD, rowSchema)
    ratingDF.printSchema()

    ratingDF.show(10, truncate = false)
    // 应用结束，关闭资源
    spark.close()
  }
}
