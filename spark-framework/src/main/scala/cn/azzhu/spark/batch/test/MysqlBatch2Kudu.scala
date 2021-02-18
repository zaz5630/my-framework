package cn.azzhu.spark.batch.test

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object MysqlBatch2Kudu {
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
//    conf.set("spark.master", "yarn")
    conf.set("spark.master","local[*]")
    conf.set("spark.app.name", "ETLFromMysql")
    val sc = new SparkContext(conf);
    sc
  }

  /** Lazily instantiated singleton instance of kuduContext */
  object KuduContextSingleton {
    @transient private var instance: KuduContext = _

    def getInstance(kuduMaster: String, sc: SparkContext): KuduContext = {
      if (instance == null) {
        instance = new KuduContext(kuduMaster, sc)
      }
      instance
    }
  }


  def main(args: Array[String]): Unit = {
    if (args.length < 9) {
      System.err.println("Usage: args参数不正确 [1]-jdbcUrl [2]-username [3]-password [4]-mysqlTableName [5]-mysql fields [6]-mysql condition [7]-tempview [8]-kuduMaster [9]-kuduTableName")
      System.exit(1)
    }

    //jdbcURL
    val jdbcUrl = args(0)
    val user = args(1)
    val password = args(2)
    val mysqlTableName = args(3)
    val fields = args(4)
    val condition = args(5)
    val tmpView = args(6)
//    val hdfsPath = args(7)
    val kuduMaster = args(7)
    val kuduTableName = args(8)


    val sc =initSparkContext()
    val session = SparkSessionSingleton.getInstance(sc.getConf)
    val kuduContext: KuduContext = KuduContextSingleton.getInstance(kuduMaster,session.sparkContext)



    val selectSql = " (SELECT "+ fields + " FROM " + mysqlTableName + " WHERE " + condition + ") AS " + tmpView  ;

    val writeOpts = Map[String, String]("createTableColumnTypes" -> "id varchar(200),deml int")

    val resultDF = session.read.format("jdbc")
      .option("url",jdbcUrl)
      .option("dbtable" , selectSql )
      .option("user",user)
      .options(writeOpts)
      .option("password",password).load()

    resultDF.show()
    resultDF.printSchema()

    val p = resultDF.selectExpr("cast(id as string) id","name_","orders","cast(deml as int)")
    p.printSchema()

    kuduContext.upsertRows(resultDF,kuduTableName)

    session.close()

  }

}

