ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.7"
ThisBuild / organization := "com.github"

lazy val root = (project in file("."))
  .settings(
    name := "hpient",
    idePackagePrefix := Some("com.github.oliverdding.hpient"),
    libraryDependencies ++= Seq(
      "com.softwaremill.sttp.client3" %% "core" % "3.3.18",
      "org.scalatest" %% "scalatest" % "3.2.10" % Test
    )
  )
