version := "0.1.0-SNAPSHOT"

scalaVersion := "2.13.12"

name := "scala code"

val sparkVersion = "3.5.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion