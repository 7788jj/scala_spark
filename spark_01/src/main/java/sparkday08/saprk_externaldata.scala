package sparkday08

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._


/*
spark加载外部数据源，parquet, orc, json, jdbc, csv
 */
object saprk_externaldata {
  def main(args: Array[String]): Unit = {
    //TODO 创建saprksession对象
    val spark: SparkSession = SparkSession.builder()
      .appName("saprk_RDD_dataframe_reflect")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._


    //todo  spark读取parquet格式文件,走sparksassion读数据直接读到dataframe或者dataset
//    val frame: DataFrame = spark.read.parquet("C:\\Users\\CYC11\\IdeaProjects\\scala_spark\\spark_01\\src\\data\\resources\\users.parquet")
//    frame.show(10, truncate = false)

    //todo spark读取json格式数据两种方法 1.直接都json文件,2.使用文本来读取json数据并转换
    val frame1: DataFrame = spark.read.json("C:\\Users\\CYC11\\IdeaProjects\\scala_spark\\spark_01\\src\\data\\resources\\people.json")
    //frame1.show(10, truncate = false)
    val value: Dataset[String] = spark.read.textFile("C:\\Users\\CYC11\\IdeaProjects\\scala_spark\\spark_01\\src\\data\\resources\\people.json")
    //value.show(10, truncate = false)
//    //TODO 将数据读取为字段名为value的一行一行的数据，需要转换为json的格式如下
//    value
//      //TODO datafram的方法select
//      .select(
//      get_json_object($"value", "$.name") as("name"),
//      get_json_object($"value", "$.age") as("age")
//    ).show(10, truncate = false)


    //TODO 读取csv格式数据要赵屹第一行的为这段，与分隔符
//    val frame2: DataFrame = spark.read
//      //表示是否第一行为字段名
//      .option("header", "true")
//      //表示读取的分隔符
//      .option("sep", "\\t")
//      //表示是否自动推断读取的数据的类型
//      .option("infoSchema", "true")
//      .csv("C:\\Users\\CYC11\\IdeaProjects\\scala_spark\\spark_01\\src\\data\\ml-100k\\u.dat")
//    frame2.show(10, truncate = false)

    //TODO 读取数据mysql表
    val frame3: DataFrame = spark.read
      .format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://master:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "cyc.score")
      .load()

    frame3.show(10, truncate = false)

    spark.stop()


  }
}
