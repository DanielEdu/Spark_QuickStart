package demo.deltalake.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Utils {

  // Schemas
  val SCHEMA_V1: StructType = StructType(
    List(
      StructField("USER_ID",IntegerType, nullable = true),
      StructField("EVENT_NAME",StringType , nullable = true)
    )
  )

  val SCHEMA_V2: StructType = StructType(
    List(
      StructField("USER_ID",IntegerType, nullable = true),
      StructField("EVENT_NAME",StringType , nullable = true),
      StructField("DELAY",IntegerType, nullable = true)
    )
  )

  //  Get Dataframe from Kafka topic
  def getEventsKafka(topic: String, spark: SparkSession): DataFrame ={
    spark.readStream
      .format("kafka")
      .option("subscribe", topic)
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", "latest")
      .load()
  }


  // Write to Delta Table
  def writeToDeltaTable(df: DataFrame, path: String, checkpointLocation: String): Unit = {
    df.writeStream
      .format("delta")
      .outputMode("append")
      .option("mergeSchema", "true")
      .option("checkpointLocation", checkpointLocation)
      .start(path)
      .awaitTermination()

  }


  // Streaming Output by Console
  def streamingOutputConsole(df: DataFrame): Unit = {
    df.writeStream
      .outputMode("update")
      .format("console")
      .start()

  }

}
