package sparkday06

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/*
sparksql的变编程有两种方式一个为DSL（datasetAPI）还有一个为SQL方式
 */
object sparksql_wordcount_DSl {
  def main(args: Array[String]): Unit = {
    //TODO 1.构建sparksesion对象
   val spark : SparkSession = SparkSession.builder()
     .appName("sparksql_wordcount")
     .master("local[2]")
     .getOrCreate()
    import spark.implicits._

    //TODO 2.读取文本内容
    val inputDS: Dataset[String] = spark.read.textFile("C:\\Users\\CYC11\\IdeaProjects\\scala_spark\\spark_01\\src\\data\\data.txt")

    //TODO 3.对读取的数据进行清洗
    val etlDF: DataFrame = inputDS
      .filter(line => line != null && line.trim.length > 0)
      .flatMap(iter => iter.split(" "))
      .groupBy("value")
      .count()

    //展示前10个，truncate表示截取前几个字符串发false表示不截取
    etlDF.show(10 , truncate = false)

    //TODO 4.关闭资源,底层技术sparkcontext.stop()
    spark.stop()
  }

}
