package sparkday10_offline_Dome.utils

import org.lionsoul.ip2region.{DataBlock, DbConfig, DbSearcher}
import sparkday10_offline_Dome.etl.Region

/**
* IP地址解析为省份和城市
* @param ip ip地址
* @param dbSearcher DbSearcher对象
* @return Region 省份城市信息
*/

object IPUtils {
  //TODO 创建ip地址转换类，对分区进行操作，传递ip与DbSearcher对象
  def IptoRegin(ip:String, searcher: DbSearcher) : Region ={
    //b. 依据IP地址解析
    val block: DataBlock = searcher.btreeSearch(ip)
    //c. 分割字符串，获取省份和城市
    val region: String = block.getRegion
    //d.拆包，空格表示不需要的这段
    val Array(_, _, province, city, _) = region.split("\\|")
    Region(ip, province, city)
  }

}
