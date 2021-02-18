package cn.azzhu.spark.batch.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Hbase2HiveContacts {

  Logger.getLogger("org").setLevel(Level.WARN)
  private val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      System.err.println("Usage: args参数不正确 [1]-phoenixUrl")
      System.exit(1)
    }
//    val userName = "admin"
//    val phoenixUrl = "jdbc:phoenix:bigdata-dev-app001:2181"
//    val userName = args(0)
    val phoenixUrl = args(0)

//    println(s"~~~~~~~ userName : $userName ~~~~~~")
    println(s"~~~~~~~ phoenixUrl : $phoenixUrl ~~~~~~")

    System.setProperty("HADOOP_USER_NAME", "admin")
    val sparkConf = new SparkConf()
    val spark = SparkSessionSingleton.getInstance(sparkConf)
//    val query = s"(SELECT MENU_ID,MENU_NAME,SOURCE,DEVELOPERNAMES,TESTERNAMES,PRODUCERNAMES,SYSTEMMENUID,SYSTEMMENUNAME,ID," +
//      s"MENUFROM,PARENTID FROM MCS_ANALYSIS.MENU_CONTACTS) tmp"
    val frame: DataFrame =  spark.read
      .format("org.apache.phoenix.spark")
      .option("table","MCS_ANALYSIS.MENU_CONTACTS")
      .option("zkUrl",phoenixUrl)
      .load()
//    val frame: DataFrame = spark.read
//      .format("jdbc")
//      .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
//      .option("url",phoenixUrl)
//      .option("dbtable",query)
//      .load()
    frame.printSchema()
    frame.createTempView("tmp")

//    frame_1.write.mode(SaveMode.Overwrite).format("parquet").save(hdfsPath)
    spark.sql("insert overwrite table vdm_mcs.v_o_menu_contacts SELECT MENU_ID,MENU_NAME,SOURCE,DEVELOPERNAMES,TESTERNAMES," +
      "PRODUCERNAMES,SYSTEMMENUID,SYSTEMMENUNAME,ID,MENUFROM,PARENTID from tmp")
    println(" vdm_mcs.v_o_menu_contacts 数据写入成功")
//    spark.sql("show databases").show(10)
    spark.close()
  }


  object SparkSessionSingleton {

    @transient private var instance: SparkSession = _
    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession
          .builder
          .appName("HBase2Hive_contacts")
          .master("yarn")
          .config(sparkConf)
//            .config("spark.sql.orc.impl","native")
//          .config("spark.sql.parquet.writeLegacyFormat", "true")
//          .config("spark.sql.warehouse.dir", "hdfs://10.121.18.6:8020/user/hive/warehouse")
//          .config("hive.metastore.uris","thrift://10.121.18.8:9083,thrift://10.121.18.9:9083")
//            .config("fs.defaultFS","10.121.18.5:8082")
//            .config("hive.metastore.warehouse.dir","hdfs://10.121.18.6:8020/user/hive/warehouse")
//          .enableHiveSupport()
          .getOrCreate()
      }
      instance
    }
  }

}
