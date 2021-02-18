package cn.azzhu.spark.batch.test

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object ETLFromMysqlPartition {
  /** Lazily instantiated singleton instance of SparkSession */
  object SparkSessionSingleton {

    @transient private var instance: SparkSession = _
    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession
          .builder
          .config(sparkConf)
          .getOrCreate()
      }
      instance
    }
  }

  /** init SparkContext */
  def initSparkContext() = {
    val conf = new SparkConf()
    conf.set("spark.master", "yarn")
    conf.set("spark.app.name", "ETLFromMysql")
    val sc = new SparkContext(conf)
    sc
  }



  def main(args: Array[String]): Unit = {
    if (args.length < 12) {
      System.err.println("Usage: args参数不正确 请输入配置文件文件名")
      System.exit(1)
    }

    //jdbcURL
    val jdbcUrl = args(0)
    val user = args(1)
    val password = args(2)
    val tableName = args(3)
    val fields = args(4)
    val condition = args(5)
    val tmpView = args(6)
    val hdfsPath = args(7)

    val partitionColumn = args(8) //按那一列进行分区 必须是数字字段
    val numPartitions = args(9) //设置多少个分区
    val lowerBound = args(10) //下界
    val upperBound = args(11) //下界



    val sc =initSparkContext()
    val session = SparkSessionSingleton.getInstance(sc.getConf)

    val selectSql = "( SELECT "+ fields + " FROM " + tableName + " WHERE " + condition + " ) AS " + tmpView
    print("selectsql == " + selectSql)

    val resultDF = session.read.format("jdbc")
      .option("url",jdbcUrl)
      //.option("dbtable","(" + selectSql + " WHERE  (updation_date >= '" + startTime + "' AND updation_date <= '" + endTime + "') " + ") AS " + tmpView)
      .option("dbtable" , selectSql )
      .option("user",user)
      .option("password",password)
      .option("partitionColumn",partitionColumn)
      .option("numPartitions",numPartitions)
      .option("lowerBound",lowerBound)
      .option("upperBound",upperBound).load()

    resultDF.write.mode(SaveMode.Overwrite).orc(hdfsPath)

    session.close()

  }

}

