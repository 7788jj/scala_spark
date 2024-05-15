//package sparkday04
//
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.hbase.HBaseConfiguration
//import org.apache.hadoop.hbase.client.Put
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable
//import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.spark.rdd.RDD
//import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.zookeeper.Environment.list
//
//
///*
//保存数据到habse
// */
//object saprk_sink {
//  def main(args: Array[String]): Unit = {
//
//    val conf: SparkConf = new SparkConf()
//      //设置主程序入口
//      .setAppName("spark_wordcount")
//      //设置为本地模式并分配两个核
//      .setMaster("local[1]")
//
//    val sc = new SparkContext(conf)
//
//
//    val inputRDD: RDD[String] = sc.textFile("C:\\Users\\CYC11\\IdeaProjects\\scala_spark\\spark_01\\src\\data\\data.txt")
//
//
//    val resultRDD: RDD[(String, Int)] = inputRDD
//      //去除读取的数据的空格与其他制表符
//      .filter(line => line != null && line.length != 0)
//      //对读取的数据进行分割
//      .flatMap(line => line.split(" "))
//      //将数据进行统计加一
//      .map(word => (word, 1))
//      //按照word来分组聚合
//      .reduceByKey((tmp, item) => tmp + item)
//
//
//
//      //保存数据到habse需要分为两个步骤
//    //TODO 1.将计算结果改为相应的数据类型的RDD，RDD[(ImutableBytesWritable, put)]
//    val putsRDD = resultRDD.mapPartitions{iter =>
//      iter.map {
//        case (word, count) =>
//          val put = new Put(Bytes.toBytes(word))
//          put.addColumn(list
//            Bytes.toBytes("info"), Bytes.toBytes("count"), Bytes.toBytes(count.toString)
//          )
//          (new ImmutableBytesWritable(put.getRow), put)
//      }
//
//    }
//
//
//
//
//    val configuration: Configuration = HBaseConfiguration.create()
//    configuration.set("hbase.zookeeper.quorum", "master")
//    configuration.set("hbase.zookeeper.property.clientPort", "2181")
//    configuration.set("zookeeper.znode.parent", "/hbase")
//    configuration.set(TableOutputFormat.OUTPUT_TABLE, "hb_wordcount")
//
//
//
//    //todo 2.保存数据到habse表
////    def saveAsNewAPIHadoopFile(
////                                path: String,
////                                keyClass: Class[_],
////                                valueClass: Class[_],
////                                outputFormatClass: Class[_ <: NewOutputFormat[_, _]],
////                                conf: Configuration = self.context.hadoopConfiguration): Unit
//
//    putsRDD.saveAsNewAPIHadoopFile(
//      path = "/wwdwdw",
//      classOf[ImmutableBytesWritable],
//      classOf[Put],
//      classOf[TableOutputFormat[ImmutableBytesWritable]],
//      configuration
//    )
//
//
//    sc.stop()
//
//
//
//
//
//  }
//
//}
