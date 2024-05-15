package sparkday06
/*
saprksql的程序入口为sparksession类似于 sparkcore的sparkcontext

 */
import org.apache.spark.sql.{Dataset, SparkSession}

object saprksql_sparksession {
  def main(args: Array[String]): Unit = {
    //TODO sparksql是建造者模式来构建sparksql的程序入口sparksession
    val spark: SparkSession = SparkSession.builder()
      //TODO 设置名称与运行模式
      .appName("saprksql_sparksession")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._

    //TODO 从文件系统读文件保存到dataset当中
    //dataset = RDD + scheme
    val inputDS: Dataset[String] = spark.read.textFile("C:\\Users\\CYC11\\IdeaProjects\\scala_spark\\spark_01\\src\\data\\data.txt")
    inputDS.printSchema()
    //TODO 获取样本数据前几个类似于take
    inputDS.show(10)


    //TODO 需要释放资源
  spark.stop()
  }

}
