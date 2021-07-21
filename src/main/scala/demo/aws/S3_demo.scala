package demo.aws

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

object S3_demo extends App {
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("org").setLevel(Level.OFF)

  val s3Dan = "s3a://danielpoc/events.json"    //filename
  val s3DanOut = "s3a://danielpoc"             // output


  val spark = SparkSession.builder
    .master("local[*]")
    .appName("readS3")
    .getOrCreate

  import org.apache.spark.sql.functions._

  spark.sparkContext
    .hadoopConfiguration
    .set("fs.s3a.access.key", "")
  // Replace Key with your AWS secret key (You can find this on IAM
  spark.sparkContext
    .hadoopConfiguration
    .set("fs.s3a.secret.key", "")
  spark.sparkContext
    .hadoopConfiguration
    .set("fs.s3a.endpoint", "s3.amazonaws.com")


  //read json file into dataframe
  val df = spark.read.json(s3Dan)
  df.printSchema()

  val df_final = df.select("$table"
    ,"_time"
    ,"_user"
    ,"properties._country_code"
    ,"properties._city"
    ,"properties._platform"
    ,"properties._os_version"
    ,"properties"
  )
    .filter(ltrim(col("$table")).like("atr_%"))
  //rakam.show(false)

  df_final
    .write
    .mode(SaveMode.Overwrite)
    .partitionBy("_country_code")
    .json(s3DanOut+"/out")

}
