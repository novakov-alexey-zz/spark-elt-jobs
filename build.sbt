import Dependencies._

Global / onChangedBuildSource := ReloadOnSourceChanges
ThisBuild / scalaVersion := "2.12.14"
ThisBuild / organization := "io.github.novakov-alexey"
ThisBuild / organizationName := "novakov-alexey"

lazy val root = (project in file("."))
  .aggregate(sparkJobs, hadoopJobs, common)
  .settings(
    assembleArtifact := false
  )

lazy val sparkJobs = (project in file("./modules/sparkjobs"))
  .dependsOn(common)
  .settings(
    name := "etl-spark-jobs",
    libraryDependencies ++= Seq(
      sparkSql % Provided,
      delta,
      scalaTest % Test
    ),
    assemblyPackageScala / assembleArtifact := false,
    console / initialCommands := s"""
    import org.apache.spark.sql.SparkSession
    val spark =
      SparkSession.builder
        .appName("sbt-shell")
        .master("local")
        .getOrCreate()
    """.stripMargin,
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

lazy val hadoopJobs = (project in file("./modules/hadoopjobs"))
  .dependsOn(common)
  .settings(
    name := "etl-hadoop-jobs",
    assemblyMergeStrategy := {
      case PathList("scala", "annotation", xs @ _*)
          if xs.headOption.exists(_.startsWith("nowarn")) =>
        MergeStrategy.first
      case x =>
        val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
        oldStrategy(x)
    },
    libraryDependencies ++= Seq(
      hadoopCommon
    )
  )

lazy val common = (project in file("./modules/common")).settings(
  name := "common",
  assembleArtifact := false,
  libraryDependencies ++= Seq(
    sparkSql % Provided,
    hadoopCommon % Provided,
    mainargs
  )
)
