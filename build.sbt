import Dependencies._

Global / onChangedBuildSource := ReloadOnSourceChanges
ThisBuild / scalaVersion := "2.12.14"
ThisBuild / organization := "org.novakov-alexey"
ThisBuild / organizationName := "novakov-alexey"

lazy val root = (project in file("."))
  .settings(
    name := "spark-jobs",
    libraryDependencies ++= Seq(
      sparkSql % Provided,
      mainargs,
      scalaTest % Test
    ),
    assemblyPackageScala / assembleArtifact := false,
    // uses compile classpath for the run task, including "provided" jar (cf http://stackoverflow.com/a/21803413/3827)
    Compile / run := Defaults
      .runTask(
        Compile / fullClasspath,
        Compile / run / mainClass,
        Compile / run / runner
      )
      .evaluated,
    Compile / runMain := Defaults
      .runMainTask(Compile / fullClasspath, Compile / run / runner)
      .evaluated
  )

console / initialCommands := s"""
import org.apache.spark.sql.SparkSession
val spark =
    SparkSession.builder
      .appName("sbt-shell")
      .master("local")
      .getOrCreate()
"""
