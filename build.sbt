
name := "SparkDelta"
version := "0.1"
scalaVersion := "2.12.13"


libraryDependencies ++= {
  val sparkV = "3.1.2"
  val deltaV = "1.0.0"
  val hadoopV = "3.3.1"
  val eventhubV = "2.3.20"

  Seq(
    "org.apache.spark" %% "spark-core" % sparkV,
    "org.apache.spark" %% "spark-sql" % sparkV,
    "org.apache.spark" %% "spark-streaming" % sparkV,
    "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkV,
    "io.delta" %% "delta-core" % deltaV,
    "org.apache.hadoop" % "hadoop-common" % hadoopV,
    "org.apache.hadoop" % "hadoop-client" % hadoopV,
    "org.apache.hadoop" % "hadoop-aws" % hadoopV,
    // azure
    "com.microsoft.azure" %% "azure-eventhubs-spark" % eventhubV
  )
}

// for azure EventHub
//dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7"
