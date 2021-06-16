import Dependencies._

Global / onChangedBuildSource := ReloadOnSourceChanges
ThisBuild / scalaVersion := "2.12.14"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "spark-jobs",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.1.2",
      "com.lihaoyi" %% "mainargs" % "0.2.1",
      scalaTest % Test
    )
  )

console / initialCommands := s"""
import org.apache.spark.sql.SparkSession
val spark =
    SparkSession.builder
      .appName("sbt-shell")
      .master("local")
      .getOrCreate()
"""

//TODO: add assembly