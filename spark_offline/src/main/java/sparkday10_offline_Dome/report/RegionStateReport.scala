package sparkday10_offline_Dome.report

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import sparkday10_offline_Dome.congfig.ApplicationConfig

import java.sql.{Connection, DriverManager, PreparedStatement}

object RegionStateReport {
  def doReport(dataFrame: DataFrame): Unit = {
    val session: SparkSession = dataFrame.sparkSession
    import session.implicits._
    //dsl分析需要导入sql函数类
    val frame: DataFrame = dataFrame
      .groupBy($"province", $"city")
      .count()
      .orderBy($"count".desc)
      .withColumn("report_date", date_sub(current_date(), 2).cast(StringType))
      frame.printSchema()
      frame.show(20, truncate = false)
    frame.coalesce(1).rdd.foreachPartition(iter => saveToMySQL(iter))

  }

  //对数据就行保存到mysql
  def saveToMySQL(datas: Iterator[Row]): Unit = {
    // a. 加载驱动类
    Class.forName(ApplicationConfig.MYSQL_JDBC_DRIVER)
    // 声明变量
    var conn: Connection = null
    var pstmt: PreparedStatement = null
    try {
      // b. 获取连接
      conn = DriverManager.getConnection(
        ApplicationConfig.MYSQL_JDBC_URL,
        ApplicationConfig.MYSQL_JDBC_USERNAME,
        ApplicationConfig.MYSQL_JDBC_PASSWORD,
      )
      // c. 获取PreparedStatement对象
      val insertSql =
        """INSERT INTO
          |itcast_ads_report.region_stat_analysis (report_date, province, city, count)
          |VALUES(?, ?, ?, ?)
          |ON DUPLICATE KEY UPDATE count= VALUES(count)"""
          .stripMargin
      pstmt = conn.prepareStatement(insertSql)
      conn.setAutoCommit(false)
      // d. 将分区中数据插入到表中，批量插入
      datas.foreach { row =>
        pstmt.setString(1, row.getAs[String]("report_date"))
        pstmt.setString(2, row.getAs[String]("province"))
        pstmt.setString(3, row.getAs("city"))
        pstmt.setLong(4, row.getAs[Long]("count"))
        // 加入批次
        pstmt.addBatch()
      }
      // TODO: 批量插入
      pstmt.executeBatch()
      conn.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (null != pstmt) pstmt.close()
      if (null != conn) conn.close()
    }
  }

}
