package demo.meetups

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, sum}

object SparkMX {
  def main(args: Array[String]): Unit = {
    println("Welcome!!! ")

    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("SparkDemoMX")
      .getOrCreate()

    val sparkVersion = spark.version
    val scalaVersion = util.Properties.versionNumberString
    val javaVersionn = System.getProperty("java.version")


    println("SPARK VERSION = " + sparkVersion)
    println("SCALA VERSION = " + scalaVersion)
    println("JAVA  VERSION = " + javaVersionn)


    if (args.length < 1) {   // check if file exists
      print("Usage: MnMcount <mnm_file_dataset>")
      sys.exit(1)
    }

    // get the M&M data set file name
    val mnmFile = args(0)

    // read the file into a Spark DataFrame
    val mnmDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(mnmFile)

    // display DataFrame
    mnmDF.show(5, truncate = false)

    // aggregate count of all colors and groupBy state and color
    // orderBy descending order
    val countMnMDF = mnmDF.select("State", "Color", "Count")
      .groupBy("State", "Color")
      .sum("Count")
      .orderBy(desc("sum(Count)"))

    // show all the resulting aggregation for all the dates and colors
    countMnMDF.show(60)
    println(s"Total Rows = ${countMnMDF.count()} \n\n")


    // find the aggregate count for California by filtering
    val caCountMnNDF = mnmDF.select("*")
      .where(col("State") === "CA")
      .groupBy("State", "Color")
      .agg(sum("Count").alias("Total"))
      .orderBy(desc("Total"))

    // show the resulting aggregation for California
    caCountMnNDF.show(10)


    // close Spark
    spark.close()


  }

}
