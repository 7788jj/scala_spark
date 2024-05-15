package sparkday07

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * 采用反射的方式将RDD转换为DataFrame与DataSet,要先定义样列类来装箱
 */
object saprk_RDD_dataframe_reflect {
  def main(args: Array[String]): Unit = {
    //TODO 创建saprksession对象
    val spark: SparkSession = SparkSession.builder()
      .appName("saprk_RDD_dataframe_reflect")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._

    //TODO 读取电影信息到RDD当中，创建sparksontext对象
    val sc : SparkContext = spark.sparkContext
    val inputRDD: RDD[String] = sc.textFile("C:\\Users\\CYC11\\IdeaProjects\\scala_spark\\spark_01\\src\\data\\ml-100k\\u.data")

    // 对RDD数据进行转换为样例类RDD【case class】
   val outputRDD: RDD[MovieRating] = inputRDD.mapPartitions{ iter =>
      iter.map{line =>
        //TODO 对数据进行拆箱封装到样列类当中
      val Array(userID, itemID, rating, timetamp) = line.split("\\t")
        MovieRating(userID, itemID, rating.toDouble, timetamp.toLong)
      }
    }

    //todo  将结果进行RDD转换为Dataframe
    val frame: DataFrame = outputRDD.toDF()
    val value: Dataset[MovieRating] = outputRDD.toDS()

    //TODO 打印scaeame信息与Dataframe的前10条信息
    frame.printSchema()
    value.printSchema()
//    root
//    |-- userId: string
//    (nullable = true)
//    |-- itemId: string
//    (nullable = true)
//    |-- rating: double
//    (nullable = false)
//    |-- timestamp: long
//    (nullable = false)
    frame.show(10, truncate = false)
    value.show(10, truncate = false)
//    +------+------+------+---------+
//    | userId | itemId | rating | timestamp |
//    +------+------+------+---------+
//    |
//    196 | 242 | 3.0 | 881250949 |
//      |
//    186 | 302 | 3.0 | 891717742 |
//      |
//    22 | 377 | 1.0 | 878887116 |
//      |
//    244 | 51 | 2.0 | 880606923 |
//      |
//    196 | 242 | 3.0 | 881250949 |
//      |
//    186 | 302 | 3.0 | 891717742 |
//      |
//    22 | 377 | 1.0 | 878887116 |
//      |
//    244 | 51 | 2.0 | 880606923 |
//      |
//    166 | 346 | 1.0 | 886397596 |
//      |
//    298 | 474 | 4.0 | 884182806 |
//      +------+------+------+---------+
//    only showing top
//    10 rows



  }

}
