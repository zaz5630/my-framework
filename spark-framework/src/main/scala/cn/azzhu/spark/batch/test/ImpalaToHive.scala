package cn.azzhu.spark.batch.test

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object ImpalaToHive {
  def main(args: Array[String]): Unit = {

//    if (args.length < 5) {
//      System.err.println("Usage: args参数不正确 [1]-impalaTable [2]-hoursql [3]-hdfsPath")
//      System.exit(1)
//    }
//
//      //jdbcURL
//
//      val impalaTable = args(0)
//      val hourSql = args(1)
//      val hdfsPath = args(2)


    val sc =initSparkContext()
    val spark = SparkSessionSingleton.getInstance(sc.getConf)

    val jdbcDF = spark.read
      .format("jdbc")
      .option("driver","com.cloudera.impala.jdbc41.Driver")
      .option("url", "jdbc:impala://SZPBS-bigdata-Prd-RT-vm01:21050")
      .option("dbtable","ods_kudu_crm.o_waybill_analysis_base")
      .load()

    print("~~~~~"+jdbcDF.count())


    jdbcDF.createOrReplaceTempView("test")
    val sql = "select * from test where updation_date between '2019-10-08 14:00:00' and '2019-10-08 15:00:00'"
    val resultDF = spark.sql(sql)
    resultDF.printSchema()

    resultDF.write.mode(SaveMode.Overwrite).format("orc").save("hdfs://szpbs-plp-hadoop05:8020/tao/hive/warehouse/ods_crm_hour.db/o_waybill_analysis_base")



    spark.close()
  }

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
    //conf.set("spark.master","local[*]")
    conf.set("spark.app.name", "ImpalaToHive")
    val sc = new SparkContext(conf);
    sc
  }

}

