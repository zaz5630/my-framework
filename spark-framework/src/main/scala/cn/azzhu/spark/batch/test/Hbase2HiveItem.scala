package cn.azzhu.spark.batch.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}


object Hbase2HiveItem {

  Logger.getLogger("org").setLevel(Level.WARN)
  private val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("Usage: args参数不正确 [1]-date [2]-phoenixUrl")
      System.exit(1)
    }
//    val date = "20191118"
//    val phoenixUrl = "jdbc:phoenix:bigdata-dev-app001:2181"
    val date = args(0)
    val phoenixUrl = args(1)
    // 将2019-11-19 改为20191119
    val subDate = date.replaceAll("-","")

    println(s"~~~~~~~ date : $date ~~~~~~")
    println(s"~~~~~~~ subDate : $subDate ~~~~~~")
    println(s"~~~~~~~ phoenixUrl : $phoenixUrl ~~~~~~")

    System.setProperty("HADOOP_USER_NAME", "admin")
    val sparkConf = new SparkConf()
    val spark = SparkSessionSingleton.getInstance(sparkConf)
//    val query = "(SELECT ID,REQUEST_TIME,SYSTEM_ID,MENU_ID,SYSTEM_NAME,MENU_NAME,USER_ID,USER_NAME,PAGE_ID," +
//           s"REQUEST_TIME_START,REQUEST_TIME_END,MENU_ACCESS FROM MCS_ANALYSIS.USER_LIVE_ITEM_$subDate) tmp"
    val table = s"MCS_ANALYSIS.USER_LIVE_ITEM_$subDate"
    val frame: DataFrame =  spark.read
      .format("org.apache.phoenix.spark")
      .option("table",table)
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

    spark.sql(s"insert overwrite table vdm_mcs.v_o_user_live_item partition(access_date='$date') SELECT ID,REQUEST_TIME,SYSTEM_ID,MENU_ID," +
      "SYSTEM_NAME,MENU_NAME,USER_ID,USER_NAME,PAGE_ID,CAST(REQUEST_TIME_START AS STRING) REQUEST_TIME_START," +
      "CAST(REQUEST_TIME_END AS STRING) REQUEST_TIME_END,CAST(MENU_ACCESS AS STRING) MENU_ACCESS from tmp")

//    val frame_1: DataFrame = spark.sql("SELECT ID,REQUEST_TIME,SYSTEM_ID,MENU_ID,SYSTEM_NAME,MENU_NAME,USER_ID,USER_NAME,PAGE_ID," +
//          "CAST(REQUEST_TIME_START AS STRING) REQUEST_TIME_START,CAST(REQUEST_TIME_END AS STRING) REQUEST_TIME_END,CAST(MENU_ACCESS AS STRING) MENU_ACCESS FROM tmp")

//    frame_1.write.mode(SaveMode.Overwrite).format("parquet").save(hdfsPath)
    println(" vdm_mcs.v_o_user_live_item 数据写入成功")
    spark.close()
  }


  object SparkSessionSingleton {

    @transient private var instance: SparkSession = _
    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession
          .builder
          .appName("HBase2Hive_item")
          .master("yarn")
          .config(sparkConf)
          .enableHiveSupport()
          .getOrCreate()
      }
      instance
    }
  }

}
