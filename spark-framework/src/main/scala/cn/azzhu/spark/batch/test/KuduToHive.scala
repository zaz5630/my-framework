package cn.azzhu.spark.batch.test

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object KuduToHive {
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
    //conf.set("spark.master","local[*]")
    conf.set("spark.app.name", "kuduToHive")
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
//    if (args.length < 5) {
//      System.err.println("Usage: args参数不正确 [1]-kuduMaster [2]-kuduTableName [3]-fields [4]-condition [5]-tmpView  [6]-hdfsPath")
//      System.exit(1)
//    }

    //jdbcURL

    val kuduMaster = args(0)
    val kuduTableName = args(1)
    val hdfsPath = args(2)
//    val fields = args(2)
//    val condition = args(3)
//    val tmpView = args(4)
//    val hdfsPath = args(5)


    val sc =initSparkContext()
    //val sc =initSparkContext()
    val session = SparkSessionSingleton.getInstance(sc.getConf)
    //val kuduContext: KuduContext = KuduContextSingleton.getInstance(kuduMaster,session.sparkContext)



    //val selectSql = " (SELECT "+ fields + " FROM " + mysqlTableName + " WHERE " + condition + ") AS " + tmpView  ;

    //val selectSql = "select * from ods_kudu_tms_followfly.o_follow_fly_waybill"
    //val writeOpts = Map[String, String]("createTableColumnTypes" -> "id varchar(200),deml int")

//    val kuduDF = session.read
//      .options(Map("kudu.master" -> "SZPBS-bigdata-Prd-RT-vm01:7051,SZPBS-bigdata-Prd-RT-vm05:7051", "kudu.table" -> "ods_kudu_tms_followfly.o_follow_fly_waybill"))
//      .option("dbtable" , selectSql )
//      .format("kudu").load()
    val kuduDF = session.read.options(Map("kudu.master" -> kuduMaster,
        "kudu.table" -> kuduTableName)).format("kudu").load()

//    kuduDF.show()
//    kuduDF.printSchema()
    kuduDF.createOrReplaceTempView("kuduTable")

    val resultDF: DataFrame = session.sql("select * from kuduTable where updation_date between '2019-09-29 00:00:00'  and '2019-09-29 23:59:59'")


    resultDF.write.mode(SaveMode.Overwrite).format("orc").save(hdfsPath)



//    val p = resultDF.selectExpr("cast(id as string) id","name_","orders","cast(deml as int)")
//    p.printSchema()
//
//    kuduContext.upsertRows(resultDF,kuduTableName)

    session.close()

  }
}
