package cn.azzhu.spark.batch

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql.EsSparkSQL

/**
  * @author azzhu
  * @since 2021-02-25 16:30
  */
class TestSpark2ES {
  def main(args: Array[String]): Unit = {
    val spark = getSparkSession()
    val dataFrame = spark.createDataFrame(Seq(
      (1, 1, "2", "5"),
      (2, 2, "3", "6"),
      (3, 2, "36", "69")
    )).toDF("id", "label", "col1", "col2")

//    EsSparkSQL.saveToEs()
//    dataFrame.saveToEs("_index/_type",Map("es.mapping.id" -> "id"))
  }

  def getSparkSession(): SparkSession = {
    val masterUrl = "local"
    val appName = "ttyb"
    val sparkconf = new SparkConf()
      .setMaster(masterUrl)
      .setAppName(appName)
      .set("es.nodes", "es的IP")
      .set("es.port", "9200")
    val Spark = SparkSession.builder().config(sparkconf).getOrCreate()
    Spark
  }

}
