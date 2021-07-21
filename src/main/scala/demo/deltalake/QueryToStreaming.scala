package demo.deltalake

import demo.deltalake.utils.Constants
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object QueryToStreaming extends App {

  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("org").setLevel(Level.OFF)

  // Configure source data path
  val loadPath = Constants.PATH

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("DeltaQuery")
    .getOrCreate

  // ------------------------------------------------------

  spark.read
    .format("delta")
    .load(loadPath)
    .createOrReplaceTempView("events")

  spark.sql("SELECT * FROM events limit 100").show()

}
