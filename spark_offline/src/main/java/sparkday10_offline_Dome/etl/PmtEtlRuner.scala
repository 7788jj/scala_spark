package sparkday10_offline_Dome.etl

import org.apache.spark.SparkFiles
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{current_date, date_sub}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.lionsoul.ip2region.{DbConfig, DbSearcher}
import sparkday10_offline_Dome.congfig.ApplicationConfig
import sparkday10_offline_Dome.utils.{IPUtils, SparkUtils}


object PmtEtlRuner {
  def main(args: Array[String]): Unit = {

    // 设置Spark应用程序运行的用户：root, 默认情况下为当前系统用户
    System.setProperty("user.name", "root")
    System.setProperty("HADOOP_USER_NAME", "root")

    //TODO:1.创建sparkSession对象，导入转换类与函数
    val spark: SparkSession = SparkUtils.createSparkSession(this.getClass)
    import spark.implicits._

    val frame: DataFrame = spark
      .read
      .json(ApplicationConfig.DATAS_PATH)
    //TODO:2.经过etl以后的dataframe产生新的Dataframe
    val newFrame: DataFrame = processData(frame)

    //TODO:3.保存datarrame到hive表当中
    savetoHive(newFrame)

    //TODO:4.应用结束关闭资源
    spark.stop()
  }

  //TODO 将一整个转换操作封装到函数里面，减少main函数的冗余
  def processData(frame: DataFrame): DataFrame = {
    val session: SparkSession = frame.sparkSession
    //对文件进行分布式缓存分发到works
//    session.sparkContext.addFile(ApplicationConfig.IPS_DATA_REGION_PATH)
    //TODO 将数据转为RDD并读取到ip地址
    val sowsRDD: RDD[Row] = frame.rdd.mapPartitions { iter =>
      //todo 创建ip解析类对象，一个分区创建一个对象
      val searcher = new DbSearcher(new DbConfig, ApplicationConfig.IPS_DATA_REGION_PATH)
      iter.map { row => {
        val ipValue: String = row.getAs[String]("ip")
        val region: Region = IPUtils.IptoRegin(ipValue, searcher)
        //TODO 将数据转为Seq来增加数据字段
        val newSeq = row.toSeq :+ region.province :+ region.city
        //todo 集合转换为row字段并返回
        val row1: Row = Row.fromSeq(newSeq)
        row1
      }
      }
    }
    //todo 将RDD转为DF(增加schema信息)
    val Schema: StructType = frame.schema
      // 获取原来DataFrame中Schema信息
      // 添加新字段Schema信息
      .add("province", StringType, nullable = true)
      .add("city", StringType, nullable = true)

    val Frame: DataFrame = session.createDataFrame(sowsRDD, Schema)
      .withColumn("date_str", date_sub(current_date(), 1).cast(StringType))
    Frame
  }
  /*
  TODO 保存数据到hive数据仓库
   */
  def savetoHive(dataFrame: DataFrame): Unit = {
    dataFrame
      .coalesce(1)
      .write
      .format("hive")
      .mode(SaveMode.Append)
      .partitionBy("date_str")
      .saveAsTable("itcast_ads.pmt_ads_info")
  }
}
