package sparkday10_offline_Dome.etl

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import sparkday10_offline_Dome.utils.SparkUtils
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import sparkday10_offline_Dome.report.AdsRegionAnalysisReport
import sparkday10_offline_Dome.report.RegionStateReport.doReport



/**
 * 针对广告点击数据，依据需求进行报表开发，具体说明如下：
 * - 各地域分布统计：region_stat_analysis
 * - 广告区域统计：ads_region_analysis
 * - 广告APP统计：ads_app_analysis
 * - 广告设备统计：ads_device_analysis
 * - 广告网络类型统计：ads_network_analysis
 * - 广告运营商统计：ads_isp_analysis
 * - 广告渠道统计：ads_channel_analysis
 */
object PmtReportRunner {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkSession实例对象
    val spark: SparkSession = SparkUtils.createSparkSession(this.getClass)
    import spark.implicits._

    // 2. 从Hive表中加载广告ETL数据，日期过滤
    val pmtDF: Dataset[Row] = spark
      .read
      .format("hive")
      .table("itcast_ads.pmt_ads_info")
      //在读取数据的时候可以过滤数据，读取昨天的数据就行分析,注意要导入包函数
      .where($"date_str".equalTo(date_sub(current_date(), 1)))
    //一个df或者ds需要使用多次需要进行缓存，后面需要释放缓存
    pmtDF.persist(StorageLevel.MEMORY_AND_DISK)

    // 3. 依据不同业务需求开发报表
    // 3.1. 地域分布统计：region_stat_analysis
    doReport(pmtDF)
    // 3.2. 广告区域统计：ads_region_analysis
    AdsRegionAnalysisReport.doReport(pmtDF)

    // 4. 应用结束，关闭资源
    pmtDF.unpersist()
    spark.stop()


  }

}
