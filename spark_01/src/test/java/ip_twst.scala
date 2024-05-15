import org.lionsoul.ip2region.{DataBlock, DbConfig, DbSearcher}

object ip_twst {
  def main(args: Array[String]): Unit = {
    //a. 创建DbSearch对象，传递字典文件
    val searcher = new DbSearcher(new DbConfig, "C:\\Users\\CYC11\\IdeaProjects\\scala_spark\\spark_01\\src\\data\\dataset\\ip2region.db")
    //b. 依据IP地址解析
    val block: DataBlock = searcher.btreeSearch("101.11.222.68")
    //c. 分割字符串，获取省份和城市
    val region: String = block.getRegion
    //d.拆包，空格表示不需要的这段
   val  Array(_, _, province, city, _) = region.split("\\|")
    println(province, city)

  }

}
