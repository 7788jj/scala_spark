package sparkday10_offline_Dome.report

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import java.util.Properties


object AdsRegionAnalysisReport {
  def doReport(dataFrame: DataFrame): Unit = {
    val session: SparkSession = dataFrame.sparkSession
    import session.implicits._

    dataFrame.createOrReplaceTempView("tmp_view_pmy")
    //基于sql来分析数据
    val frame: DataFrame = session.sql(ReportSQLConstant.reportAdsRegionSQL("tmp_view_pmy"))
    frame.createOrReplaceTempView("tmp_view_report")
    val resoultFrame: DataFrame = session.sql(ReportSQLConstant.reportAdsRegionRateSQL("tmp_view_report"))


    //保存数据到mysql
    resoultFrame.write
      .mode(SaveMode.Overwrite)
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .jdbc(
        "jdbc:mysql://master:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode = true",
        "itcast_ads_report.ads_region_analysis",
        new Properties()
      )


  }


}

