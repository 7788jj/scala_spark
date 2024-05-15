package sparkday04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager, PreparedStatement}

/*
保存数据到mysql数据库。读取使用后面的dataframe
 */
object saprk_mysql {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("saprk_mysql")
      .setMaster("local[2]")
     val sc = new SparkContext(conf)
    val inputRDD: RDD[String] = sc.textFile("C:\\Users\\CYC11\\IdeaProjects\\scala_spark\\spark_01\\src\\data\\data.txt")

    val etlRDD: RDD[(String, Int)] = inputRDD
      .flatMap { iter =>
        iter.split("\\s+")
      }
      .filter(iter =>
      iter.trim.length > 0)
      .mapPartitions{iter =>
        iter.map(iter =>
          (iter, 1)
        )}

    val resoultRDD: RDD[(String, Int)] = etlRDD
      .reduceByKey((tmp, iter) => tmp + iter)

    resoultRDD.foreachPartition{iter => saveToMySQL(iter)}



    sc.stop()


    def saveToMySQL(datas: Iterator[(String, Int)]): Unit = {
      // a. 加载驱动类
      Class.forName("com.mysql.cj.jdbc.Driver")
      // 声明变量
      var conn: Connection = null
      var pstmt: PreparedStatement = null
      try {
        // b. 获取连接
        conn = DriverManager.getConnection(
          "jdbc:mysql://master:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode = true",
        "root",
        "123456"
        )
        // c. 获取PreparedStatement对象
        val insertSql = "INSERT INTO cyc.tb_wordcount (word, count) VALUES(?, ?)"
        pstmt = conn.prepareStatement(insertSql)
        conn.setAutoCommit(false)
        // d. 将分区中数据插入到表中，批量插入
        datas.foreach { case (word, count) =>
          pstmt.setString(1, word)
          pstmt.setLong(2, count.toLong)
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

}
