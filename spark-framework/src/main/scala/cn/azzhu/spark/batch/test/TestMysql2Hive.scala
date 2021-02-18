package cn.azzhu.spark.batch.test

import java.util.Properties

import org.apache.spark.sql.SparkSession


object TestMysql2Hive {

  def main(args: Array[String]): Unit = {



    /** ----- init args value start ----- **/
//    if (args.length < 1 || args.length !=2 || args.length!=4) {
//      System.err.println("Usage: args参数不正确 [1]-config properties")
//      System.exit(1)
//    }
    var configFile = "";
    var etlTimeStartTime = ""
    var etlTimeEndTime = ""
    var etl_date = ""


    if(args.length == 2) {
      println("22222222")
      configFile = args(0)
      etl_date = args(1)
    }
    if (args.length == 4) {
      println("44444444")
      configFile = args(0)
      etlTimeStartTime = args(1)
      etlTimeEndTime = args(2)
      etl_date = args(3)
    }


    println("args.length" + args.length)
    println("args[0]" + args(0))
    println("args[1]" + args(1))
    println("args[2]" + args(2))
    println("args[3]" + args(3))
    println("--------")

    println("configFile" + configFile)
    println("etlTimeStartTime" + etlTimeStartTime)
    println("etlTimeEndTime" + etlTimeEndTime)
    println("etl_date" + etl_date)


    val Array(mysqlurl,username, password, sparkSqlWarehouseDir,mysqlselectSql,mysqlInsertHiveSql) = parseArgsFromFile(configFile)

   val spark =  SparkSession
      .builder()
      .appName("Java Spark Hive Example")
//      .master("local[*]")
//      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("spark.sql.warehouse.dir", sparkSqlWarehouseDir)
//      .config("hadoop.home.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate();

    var resultSqlselectSql = mysqlselectSql

    if(etlTimeStartTime!=null && etlTimeStartTime.length > 0 && etlTimeEndTime != null && etlTimeEndTime.length > 0)  {
      resultSqlselectSql = mysqlselectSql.replace( ":operate_time_start" , "'" + etlTimeStartTime + "'")
        .replace(":operate_time_end", "'" + etlTimeEndTime + "'")
    }
    println("resultSqlselectSql" + resultSqlselectSql)

    var resultMysqlInsertHiveSql = mysqlInsertHiveSql
    if(etl_date!=null && etl_date.length > 0) {
      resultMysqlInsertHiveSql = resultMysqlInsertHiveSql.replace( ":etl_date" , "'" + etl_date + "'")
    }
    println("resultMysqlInsertHiveSql" + resultMysqlInsertHiveSql)


    val mysqlResultDF = spark.read.format("jdbc")
      .option("url",mysqlurl)
      .option("driver","com.mysql.jdbc.Driver")
      .option("dbtable" , resultSqlselectSql )
      .option("user",username)
      .option("password",password)
      .load()
    mysqlResultDF.createOrReplaceTempView("mysqlTempView")

    spark.sql(resultMysqlInsertHiveSql)
    spark.close()
  }

  def parseArgsFromFile(fileName: String): Array[String] = {
    val properties: Properties = new Properties
    import java.io.{BufferedInputStream, FileInputStream}
    val in = new BufferedInputStream(new FileInputStream(fileName))
    properties.load(in)
    //需要获取的参数
    val paramNames = Array("mysqlurl","username", "password", "spark.sql.warehouse.dir","mysqlselectSql","mysqlInsertHiveSql")
    val paramValues = paramNames.map { x =>
      properties.getProperty(x)
    }
    paramValues
  }

}
