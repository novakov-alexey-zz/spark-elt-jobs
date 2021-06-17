import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.2.8"
  lazy val sparkSql = "org.apache.spark" %% "spark-sql" % "3.1.2" 
  lazy val mainargs = "com.lihaoyi" %% "mainargs" % "0.2.1"
}
