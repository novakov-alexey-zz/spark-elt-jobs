import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.2.8"
  lazy val sparkSql = "org.apache.spark" %% "spark-sql" % "3.1.2"
  lazy val delta = "io.delta" %% "delta-core" % "1.0.0"
  lazy val hadoopCommon = "org.apache.hadoop" % "hadoop-common" % "3.2.0"
  lazy val mainargs = "com.lihaoyi" %% "mainargs" % "0.2.1"
}
