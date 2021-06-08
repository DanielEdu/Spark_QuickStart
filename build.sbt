
name := "SparkDelta"
version := "0.1"
scalaVersion := "2.12.13"


libraryDependencies ++= {
  val sparkV = "3.1.2"
  val deltaV = "1.0.0"

  Seq(
    "org.apache.spark" %% "spark-core" % sparkV,
    "org.apache.spark" %% "spark-sql" % sparkV,
    "io.delta" %% "delta-core" % deltaV
  )
}