ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.7"
ThisBuild / organization := "com.github"

val arrowVersion = "6.0.1"

lazy val root = (project in file("."))
  .settings(
    name := "hpient",
    idePackagePrefix := Some("com.github.oliverdding.hpient"),
    libraryDependencies ++= Seq(
      // Apache Spark
      "org.apache.spark" %% "spark-core" % "3.2.0",
      "org.apache.spark" %% "spark-sql" % "3.2.0" % "provided",
      // Apache Arrow
//      "org.apache.arrow" % "arrow-compression" % arrowVersion,
      "org.apache.arrow" % "arrow-format" % arrowVersion,
      "org.apache.arrow" % "arrow-vector" % arrowVersion,
      "org.apache.arrow" % "arrow-memory" % arrowVersion,
      // STTP
      "com.softwaremill.sttp.client3" %% "core" % "3.3.18",
      // Scala Test
      "org.scalatest" %% "scalatest" % "3.2.10" % Test
    )
  )
