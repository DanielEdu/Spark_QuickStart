package demo.deltalake

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object BatchDemo extends App {

  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("org").setLevel(Level.OFF)

  // Configure source data path
  val deltaPath = "./tmp/iWildcam"
  val sourcePath = "/Users/user/data_source/parquet/iWildcam_2021/Train.parquet"

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("DeltaDemo")
    .getOrCreate

  // ------------------------------------------------------
/*
  // Create the Delta table with the same loans data
  spark
    .read
    .format("parquet")
    .load(sourcePath)
    .write
    .format("delta")
    .save(deltaPath)
*/

  // Create a view on the data called iwildcam
  spark
    .read
    .format("delta")
    .load(deltaPath)
    .createOrReplaceTempView("mivista")

  // iwildcam row count
  spark.sql("SELECT count(*) FROM mivista").show()

  // First 5 rows of iwildcam table
  spark.sql("SELECT * FROM mivista LIMIT 5").show()


}
