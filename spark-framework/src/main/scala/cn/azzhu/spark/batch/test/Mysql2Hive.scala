//package com.kye.bigdata.batch
//
//import java.util.Properties
//
//import com.kye.bigdata.batch.ETLFromMysql.{SparkSessionSingleton, initSparkContext}
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.SparkSession
//
//
///**
//  * @author azzhu
//  * @since 2021-02-12 11:10
//  */
//object Mysql2Hive {
//  def main(args: Array[String]): Unit = {
//    /** ----- init args value start ----- **/
//    if (args.length < 1) {
//      System.err.println("Usage: args参数不正确 [1]-config properties")
//      System.exit(1)
//    }
//
//    val configFile = args(0)
//    val Array(mysqlurl, username, password, sparkSqlWarehouseDir, fsDefaultFS, hiveMetastoreUris, hiveMetastoreWarehouseDir,mysqlselectSql) = parseArgsFromFile(configFile)
//
//    val sc =initSparkContext()
//    val session = SparkSessionSingleton.getInstance(sc.getConf,sparkSqlWarehouseDir, fsDefaultFS, hiveMetastoreUris, hiveMetastoreWarehouseDir)
//
//    val mysqlResultDF = session.read.format("jdbc")
//      .option("url",mysqlurl)
//      //.option("dbtable","(" + selectSql + " WHERE  (updation_date >= '" + startTime + "' AND updation_date <= '" + endTime + "') " + ") AS " + tmpView)
//      .option("dbtable" , mysqlselectSql )
//      .option("user",username)
//      .option("password",password)
//      .load()
//
//    mysqlResultDF.createOrReplaceTempView("mysqlTempView")
//
//    session.sql("insert into ods_kkbmall.order_info partition(etl_date='20201001') select *  from mysqlTempView")
//    session.close()
//
//  }
//
//  def parseArgsFromFile(fileName: String): Array[String] = {
//    val properties: Properties = new Properties
//    import java.io.{BufferedInputStream, FileInputStream}
//    val in = new BufferedInputStream(new FileInputStream(fileName))
//    properties.load(in)
//    //需要获取的参数
//    val paramNames = Array("mysqlurl", "username", "password", "spark.sql.warehouse.dir",
//      "fs.defaultFS", "hive.metastore.uris", "hive.metastore.warehouse.dir","mysqlselectSql")
//    val paramValues = paramNames.map { x =>
//      properties.getProperty(x)
//    }
//    paramValues
//  }
//
//  object SparkSessionSingleton {
//    @transient private var instance: SparkSession = _
//
//    def getInstance(sparkConf: SparkConf, sparkSqlWarehouseDir: String, fsDefaultFS: String,hiveMetastoreUris:String,hiveMetastoreWarehouseDir:String): SparkSession = {
//      if (instance == null) {
//        instance = SparkSession
//          .builder
//          .config(sparkConf)
////          .config("fs.defaultFS", fsDefaultFS)
//          .config("hive.metastore.uris", hiveMetastoreUris)
//          .config("hive.metastore.warehouse.dir", hiveMetastoreWarehouseDir)
//          .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
//          .config("hadoop.home.dir", "/user/hive/warehouse")
////          .config("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", true)
////          .config("spark.hive.mapred.supports.subdirectories", true)
////          .config("hive.strict.managed.tables", false)
////          .config("hive.create.as.insert.only", false)
////          .config("metastore.create.as.acid", false)
//          .enableHiveSupport()
//          .getOrCreate()
//      }
//      instance
//    }
//  }
//
//  /** init SparkContext */
//  def initSparkContext() = {
//    val conf = new SparkConf()
//    conf.set("spark.master", "local[*]")
//    conf.set("spark.app.name", "ETLFromMysql")
//    val sc = new SparkContext(conf)
//    sc
//  }
//
//}
