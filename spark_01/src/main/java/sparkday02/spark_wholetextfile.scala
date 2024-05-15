package sparkday02
/*/
小文件读取与设置分区
 */

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_wholetextfile {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("spark_wholetextfile")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

//wholetsxtfile专门用于读取小文件(文件名为key，一行一行的数据为value)，默认一个分区为一个block块，可以设置最小分区数
    val inputRDD: RDD[(String, String)] = sc.wholeTextFiles("D:\\大数据spark资料\\ratings100", minPartitions = 2)


    inputRDD.take(1)
      .foreach(println)



  }

}
