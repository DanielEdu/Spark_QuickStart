package delta.demo
import delta.demo.utils.{Utils, Constants}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.StringType


object StreamingDemo extends App {
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("org").setLevel(Level.OFF)
  val path = Constants.PATH
  val checkpointLocation_v2 = Constants.CHECKPOINT_V2
  val checkpointLocation = Constants.CHECKPOINT

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("DeltaDemo")
    .getOrCreate

  import spark.implicits._

  def castRowEventsKafka(dataFrame: DataFrame): Dataset[(String, String)]={
    dataFrame.selectExpr("CAST(key AS STRING)", "CAST(value as STRING)")
      .as[(String, String)]
  }

  val delta_in: DataFrame = Utils.getEventsKafka("delta_demo", spark)
  val delta_out: DataFrame = Utils.getEventsKafka("delta_out", spark)

  val delta_in2 = castRowEventsKafka(delta_in)
  val delta_out2 = castRowEventsKafka(delta_out)


  //  -------------------------
  val myDf = delta_in2.select(from_json($"value".cast(StringType), Utils.SCHEMA_V1))
    .withColumnRenamed("from_json(CAST(value AS STRING))", "json")
    .select("json.USER_ID","json.EVENT_NAME")
  val myDf2 = delta_out2.select(from_json($"value".cast(StringType), Utils.SCHEMA_V2))
    .withColumnRenamed("from_json(CAST(value AS STRING))", "json")
    .select("json.USER_ID","json.EVENT_NAME", "json.DELAY")

  // Print Console
  Utils.streamingOutputConsole(myDf)
  Utils.streamingOutputConsole(myDf2)

  // Print Console and Write Delta Lake
  //Utils.writeToDeltaTable(myDf, path, checkpointLocation)

  // Another Kafka TOPIC
  Utils.writeToDeltaTable(myDf2, path, checkpointLocation_v2)

}
