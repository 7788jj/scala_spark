package sparkday08

import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.{avg, round}


/*
saprkSql集成hive
 */
object saprk_hive {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("saprk_hive")
      .master("local[2]")
      //TODO 配置hive的元数据信息位置
      .config("hive.metastore.uris", "thrift://master:9083")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

//读取hive的表数据，有两种方式，1.sql编程 2，dsl编程
    //TODO sql编程
    val frame: DataFrame = spark.sql(
      """
        |select * from cyc.employee
        |""".stripMargin)
    frame
      .show(10, truncate = false)


    //TODO dsl编程
    val frame1: DataFrame = spark.read.table("cyc.employee")
      .groupBy($"dept")
      .agg(round(avg($"salary"), 2).as("avg_salary"))
    frame1
      .show(10)












  }

}
