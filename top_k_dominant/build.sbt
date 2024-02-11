ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.11"

lazy val root = (project in file("."))
  .settings(
    name := "dominant.topK"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"
