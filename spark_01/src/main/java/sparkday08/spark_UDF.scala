package sparkday08

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

/*
在spark当中来定义函数给表来使用
 */
object spark_UDF {
  def main(args: Array[String]): Unit = {
    //创建sparkssion对象
    val spark: SparkSession = SparkSession.builder()
      .appName("spark_UDF")
      .master("local[2]")
      //TODO 配置hive的元数据信息位置
      .config("hive.metastore.uris", "thrift://master:9083")
      .enableHiveSupport()
      .getOrCreate()

    //导入隐式转换类
    import spark.implicits._


    //todo 1.sql方式来定义函数,使用register来注册

    spark.udf.register(
      "lowerss",
      (name: String) => {
        name.toUpperCase
      }
    )
    val frame: DataFrame = spark.read.table("cyc.employee")
    val frame1: DataFrame = spark.sql(
      """
        |select name, lowerss(name) as NAME from cyc.employee
        |""".stripMargin)
    frame1
      .show(10)


    //todo 2.dsl方式来使用udf函数
    val to_lower = udf(
      (name :String)=>{
        name.toUpperCase
      }
    )

    frame.select(
      $"name",
      to_lower($"name").as("NAME")
    )
      .show(10)


  }

}
