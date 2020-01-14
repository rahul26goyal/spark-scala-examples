name := "spark-scala-examples"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion)