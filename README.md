# _Spark-Streaming-Template-Lite_
A barebones project with scala, apache spark built using sbt. Spark-shell provides `spark` and `sc` variables pre-initialised.

## Prerequisites
- [Java 1.8](https://java.com/en/download/)
- [Scala 2.12.13](https://www.scala-lang.org/)
- [Spark 3.1.2](https://spark.apache.org/)
- [sbt 1.3.12](https://www.scala-sbt.org/)

## Build and Demo process

- `clean`	Deletes all generated files (in the target directory).
- `compile`	Compiles the main sources (in src/main/scala and src/main/java directories).
- `test`	Compiles and runs all tests.
- `console`	Starts the Scala interpreter with a classpath including the compiled sources and all dependencies. To return to sbt, type :quit, Ctrl+D (Unix), or Ctrl+Z (Windows).
- `run <argument>*`	Runs the main class for the project in the same virtual machine as sbt.
- `package`	Creates a jar file containing the files in src/main/resources and the classes compiled from src/main/scala and src/main/java.
- `help <command>`	Displays detailed help for the specified command. If no command is provided, displays brief descriptions of all commands.
- `reload`	Reloads the build definition (build.sbt, project/*.scala, project/*.sbt files). Needed if you change the build definition.

## What the demo does?
...




## Using this Repo
Just import it into your favorite IDE as a sbt project. Tested with IntelliJ to work. Or use your favorite editor and build from command line with sbt.

## Libraries Included


## Useful Links
- [Spark Docs - Root Page](http://spark.apache.org/docs/latest/)
- [Spark Programming Guide](http://spark.apache.org/docs/latest/programming-guide.html)
- [Spark Latest API docs](http://spark.apache.org/docs/latest/api/)
- [Scala API Docs](https://docs.scala-lang.org/getting-started/index.html)



### Quick-Start Delta Lake 1.0.0
### Quick-Start Spark Streaming
### Quick-Start batch S3
### Quick-Start Azure EventHub
###

- SPARK VERSION = 3.1.2
- SCALA VERSION = 2.12.13
- JAVA  VERSION = 1.8.0_275