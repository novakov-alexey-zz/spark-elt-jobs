import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.2.8"
  lazy val sparkSql = "org.apache.spark" %% "spark-sql" % "3.1.2"
  lazy val delta = "io.delta" %% "delta-core" % "1.0.0"
  lazy val mainargs = "com.lihaoyi" %% "mainargs" % "0.2.1"
  lazy val circeCore = "io.circe" %% "circe-core" % "0.14.1"
  lazy val circeParser = "io.circe" %% "circe-parser" % "0.14.1"
  lazy val circeGeneric = "io.circe" %% "circe-generic" % "0.14.1"
  lazy val awsCore = "com.amazonaws" % "aws-lambda-java-core" % "1.2.1"
  lazy val log4jWeb = "org.apache.logging.log4j" % "log4j-web" % "2.14.1"

  def hadoopS3Dependencies(version: String): Seq[ModuleID] = {
    Seq(
      ("org.apache.hadoop" % "hadoop-aws" % version)
        .exclude("com.amazonaws", "aws-java-sdk-bundle"),
      "com.amazonaws" % "aws-java-sdk-s3" % AwsSdkVer,
      "com.amazonaws" % "aws-java-sdk-dynamodb" % AwsSdkVer
    )
  }

  // Glue: Spark & Hadoop
  lazy val glueSpark = "org.apache.spark" %% "spark-sql" % "2.4.3"
  lazy val awsGlue = "com.amazonaws" % "AWSGlueETL" % "1.0.0"
  lazy val glueHadoopVer = "2.8.5"
  lazy val glueHadoopS3Dependencies: Seq[sbt.ModuleID] = hadoopS3Dependencies(
    glueHadoopVer
  )
  lazy val glueHadoopCommon =
    "org.apache.hadoop" % "hadoop-common" % glueHadoopVer

  // Hadoop Dependencies
  lazy val HadoopVer = "3.2.0"
  lazy val hadoopS3Dependencies: Seq[sbt.ModuleID] = hadoopS3Dependencies(
    HadoopVer
  )
  lazy val AwsSdkVer =
    "1.11.375" //aws sdk must match to Hadoop transitive dependency version, for example aws 1.11.375 depends on hadoop 3.2.0

  lazy val hadoopCommon = "org.apache.hadoop" % "hadoop-common" % HadoopVer
}
