package cn.azzhu.spark.batch.test

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object ETLFromMysql {
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
    if (args.length < 8) {
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


    val sc =initSparkContext()
    val session = SparkSessionSingleton.getInstance(sc.getConf)

    val selectSql = " (SELECT "+ fields + " FROM " + tableName + " WHERE " + condition + ") AS " + tmpView  ;
    print("selectsql == " + selectSql)

    val resultDF = session.read.format("jdbc")
      .option("url",jdbcUrl)
      //.option("dbtable","(" + selectSql + " WHERE  (updation_date >= '" + startTime + "' AND updation_date <= '" + endTime + "') " + ") AS " + tmpView)
      .option("dbtable" , selectSql )
      .option("user",user)
      .option("password",password).load()

    resultDF.write.mode(SaveMode.Overwrite).format("orc").save(hdfsPath)

    session.close()

  }

}

