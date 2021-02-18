package cn.azzhu.spark.streaming

/**
  * @author azzhu
  * @since 2021-02-14 11:29
  */
import java.util.Properties

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kudu.client.KuduClient
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object SparkStreamingKudu {

  def main(args: Array[String]): Unit = {
    /** ----- init args value start ----- **/
    if (args.length < 1) {
      System.err.println("Usage: args参数不正确 [1]-config properties")
      System.exit(1)
    }

    val configFile = args(0)
    /** ----- init args value stop ----- **/



    val Array(kafkaBrokers,kafkaTopics, kuduMaster, kuduTableName, seconds, debugFlag, groupId,mysqlTableName,filterSql,selectExpr,iudSql,autooffsetreset) = parseArgsFromFile(configFile)


    val ssc = createContext(kafkaBrokers, kafkaTopics, kuduMaster, kuduTableName, seconds.toInt, groupId, debugFlag.toBoolean, mysqlTableName,filterSql,selectExpr,iudSql,autooffsetreset)
    ssc.start()
    LogManager.getRootLogger.setLevel(Level.WARN)
    ssc.awaitTermination()
  }

  //计算主体
  def createContext(kafkaBrokers: String, kafkaTopics: String, kuduMaster: String, kuduTableName: String, interval: Int, groupId: String, debugFlag: Boolean, mysqlTableName: String,filterSql:String,selectExpr:String,iudSql:String,autooffsetreset:String) = {
    val ssc = initSparkStreamingContext(interval)
    val bc_debug = ssc.sparkContext.broadcast(debugFlag)
    //多个topic使用,分隔

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaBrokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> autooffsetreset,
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    val topics = Array(kafkaTopics)
    val dStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    dStream.foreachRDD(rdd => {

      if (!rdd.isEmpty()) {
        //        try {
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
        val kuduContext = KuduContextSingleton.getInstance(kuduMaster, spark.sparkContext)



        import spark.implicits._


        val jsonDS = rdd.map(record => record.value()).toDS
        val allDF: DataFrame = spark.read.json(jsonDS)
        allDF.createOrReplaceTempView("allResult")

        spark.sql("select * from allResult").show()


//        val filterDF: DataFrame = spark.sql(filterSql)
        ////        filterDF.createOrReplaceTempView("filterTable")
        ////
        ////        val iudResultDF: DataFrame = spark.sql(selectExpr)
        ////        iudResultDF.createOrReplaceTempView("result")
        //
        //
        ////
        ////        val insertDF: DataFrame = spark.sql(iudSql + " WHERE type = 'INSERT' ")
        ////        val updateDF: DataFrame = spark.sql(iudSql + " WHERE type = 'UPDATE' ")
        ////        val deleteDF: DataFrame = spark.sql(iudSql + " WHERE type = 'DELETE' ")
        ////
        ////        if(insertDF!=null && !insertDF.take(1).isEmpty) {
        ////          kuduContext.upsertRows(insertDF,kuduTableName)
        ////        }
        ////
        ////        if(updateDF!=null && !updateDF.take(1).isEmpty) {
        ////          kuduContext.upsertRows(updateDF,kuduTableName)
        ////        }
//
//        if(deleteDF!=null && !deleteDF.take(1).isEmpty) {
//          kuduContext.deleteRows(deleteDF,kuduTableName)
//        }


        dStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)


        //        } catch {
        //          case t: Throwable => t.toString
        //
        //        }

      }
    })

    ssc
  }

  /** initSparkStreamingContext **/
  def initSparkStreamingContext(interval: Int) = {
    val conf = new SparkConf()
      .setAppName("MysqlKafkaKudu-SparkStreaming")
    .setMaster("local[*]")
    //启用反压机制
    conf.set("spark.streaming.backpressure.enabled","true")
    //最小摄入条数控制
    conf.set("spark.streaming.backpressure.pid.minRate","1")
    //最大摄入条数控制
    conf.set("spark.streaming.kafka.maxRatePerPartition","500")
    //初始最大接收速率控制
    conf.set("spark.streaming.backpressure.initialRate","100")
    //
    conf.set("spark.streaming.kafka.consumer.poll.ms", "3000")
    conf.set("spark.task.maxFailures","10")
    //优雅停机
    conf.set("spark.streaming.stopGracefullyOnShutdown","true")



    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(interval))
    ssc
  }

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

  /** Lazily instantiated singleton instance of kuduContext */
  object KuduClientSingleton {
    @transient private var instance: KuduClient = _

    def getInstance(kuduMaster: String): KuduClient = {
      if (instance == null) {
        instance = new KuduClient.KuduClientBuilder(kuduMaster).defaultAdminOperationTimeoutMs(1000).build()

      }
      instance
    }
  }

  def parseArgsFromFile(fileName: String): Array[String] = {
    val properties: Properties = new Properties
    import java.io.{BufferedInputStream, FileInputStream}
    val in = new BufferedInputStream(new FileInputStream(fileName))
    properties.load(in)
    //需要获取的参数
    val paramNames = Array("kafkaBrokers","kafkaTopics", "kuduMaster", "kuduTableName", "seconds", "debugFlag", "groupId","mysqlTableName","filterSql","selectExpr","iudSql","autooffsetreset")
    val paramValues = paramNames.map { x =>
      properties.getProperty(x)
    }
    paramValues
  }
}

