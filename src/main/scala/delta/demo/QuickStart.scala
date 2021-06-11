package delta.demo
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import io.delta.tables._
import org.apache.spark.sql.functions._

object QuickStart extends App{

  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("DeltaDemo")
    .getOrCreate

  //  -------------------------------------------------------------
  val deltaPath = "./tmp/loans_delta"

/*
  // Create Data
  val data = spark.range(0, 5)
  data.write.format("delta").save(deltaPath)

  val df = spark.read.format("delta").load(deltaPath)
  df.show()


  // Overwrite
  println("----  Overwrite  ----")
  val data_v2 = spark.range(5, 10)
  data_v2.write
    .format("delta")
    .mode("overwrite")
    .save(deltaPath)

  // Read Data
  val df_v2 = spark.read.format("delta").load(deltaPath)
  df_v2.show()


  // Read older version of data using time travel
  println("----  Time Travel  ----")
  val df_rb = spark.read.format("delta").option("versionAsOf", 0).load(deltaPath)
  df_rb.show()
*/

  // Append
  println("----  Append  ----")
  val data_v3 = spark.range(11, 15)
  data_v3.write
    .format("delta")
    .mode("append")
    .save(deltaPath)

  val df_v3 = spark.read.format("delta").load(deltaPath)
  df_v3.show()


}
