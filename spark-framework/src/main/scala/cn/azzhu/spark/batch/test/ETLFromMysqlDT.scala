package cn.azzhu.spark.batch.test

import java.util.Properties

import org.apache.spark.sql.{DataFrame, _}
import org.apache.spark.{SparkConf, SparkContext, sql}

import scala.collection.mutable.ListBuffer


object ETLFromMysqlDT {

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

  def initSparkContext() = {
    val conf = new SparkConf()
//    conf.set("spark.master", "local[*]")
    conf.set("spark.master", "yarn")
    conf.set("spark.app.name", "ETLFromMysqlDT")
    val sc = new SparkContext(conf)
    sc
  }

  def createMysqlTempView(url: String, userName: String, password: String, sqls:List[String],
                          spark: SparkSession): List[(String,sql.DataFrame)] = {
    val proBasicData = new Properties()
    proBasicData.put("user", userName)
    proBasicData.put("password", password)
    proBasicData.put("driver","com.mysql.jdbc.Driver")

    val pattern="\\bfrom\\s*\\S*".r
    val dfs = for {
      sql <- sqls
    } yield (pattern.findFirstIn(sql).get.replaceAll("\\s{1,}", " ")
      .split(" ")(1).replaceAll("\\s*\\S*\\.",""), spark.read.jdbc(url,sql,proBasicData))

    dfs

    //      for {
    //        (sql, df) <- dfs
    //      } df.createOrReplaceTempView(sql)
  }

  def readFromMysql(url: String, userName: String, password: String, sql:String,
                    spark: SparkSession): DataFrame = {
    val proBasicData = new Properties()
    proBasicData.put("user", userName)
    proBasicData.put("password", password)
    proBasicData.put("driver", "com.mysql.jdbc.Driver")

    val frame: DataFrame = spark.read.jdbc(url,sql,proBasicData)
    frame
  }

  def readAllFromMysql(url: String, userName: String, password: String, sqls:List[String],
                       spark: SparkSession): DataFrame = {
    val proBasicData = new Properties()
    proBasicData.put("user", userName)
    proBasicData.put("password", password)
    proBasicData.put("driver", "com.mysql.jdbc.Driver")

    var seqFrame = Seq[DataFrame]()
    for (sql <- sqls) {
      val frame = spark.read.jdbc(url,sql,proBasicData)
      seqFrame +:= frame
    }
    val finalFrame: DataFrame = seqFrame.reduce(_ union _)
    finalFrame
  }

  def main(args: Array[String]): Unit = {

    if (args.length < 8) {
      System.err.println("Usage: args参数不正确 [1]-hadoopUserName [2]-url [3]-userName [4]-password" +
        "[5]-tableName [6]-cnt [7]-hdfsPath [8]-sql [9]-condition")
      System.exit(1)
    }
    val hadoopUserName = args(0)
    val url = args(1)
    val userName = args(2)
    val password = args(3)
    val prefixTable = args(4)
    val cnt = args(5)
    val hdfsPath = args(6)
    val sql = args(7)
    val condition = args(8)

    System.setProperty("HADOOP_USER_NAME", hadoopUserName)
    val sc =initSparkContext()
    val session = SparkSessionSingleton.getInstance(sc.getConf)

//    val url = "jdbc:mysql://10.121.7.15:3306/erp_dispatch_9?characterEncoding=UTF-8"
//    val userName = "bigdata_chenliang_alldb_read"
//    val password = "7a91h34l8zEgV1K9"

//    val prefixTable = "dispatch_warn_"
//    val cnt = 100

    val sqlList = new ListBuffer[String]
    for (i <- 0 until cnt.toInt) {
      sqlList.append("(select " + sql + " from " + prefixTable + i + " where "+ condition + ") as tmp")
    }

    val sqls = sqlList.toList
    val frame: DataFrame = readAllFromMysql(url,userName,password,sqls,session)
    frame.printSchema()
    frame.show(2)
    frame.write.mode(SaveMode.Overwrite).format("orc").save(hdfsPath)

    session.close()
    sc.stop()

  }

}
