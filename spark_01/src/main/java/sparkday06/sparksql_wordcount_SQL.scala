package sparkday06

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/*
sparksql的变编程有两种方式一个为DSL（datasetAPI）还有一个为SQL方式
 */
object sparksql_wordcount_SQL {
  def main(args: Array[String]): Unit = {
    //TODO 1.构建sparksesion对象
   val spark : SparkSession = SparkSession.builder()
     .appName("sparksql_wordcount")
     .master("local[2]")
     .getOrCreate()
    import spark.implicits._

    //TODO 2.读取文本内容
    val inputDS: Dataset[String] = spark.read.textFile("C:\\Users\\CYC11\\IdeaProjects\\scala_spark\\spark_01\\src\\data\\data.txt")

    //TODO 4.对读取的数据进行清洗
    val etlDS: Dataset[String] = inputDS
      .filter(line => line != null && line.trim.length > 0)
      .flatMap(iter => iter.split(" "))

    //打印表信息,scheme就是表的这段名称与类型
    etlDS.printSchema()


    //TODO 5.将分割好的数据注册为视图，并写sql
    etlDS.createOrReplaceTempView("view_word")
    val outputDF: DataFrame = spark.sql(
      """
        |select value, count(1) as total from view_word group by value order by total desc
        |""".stripMargin)


    //展示前10个，truncate表示截取前几个字符串发false表示不截取
    outputDF.show(10 , truncate = false)

    //TODO 6.关闭资源,底层技术sparkcontext.stop()
    spark.stop()
  }

}
