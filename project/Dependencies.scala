import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.2.8"
  lazy val sparkSql = "org.apache.spark" %% "spark-sql" % "3.1.2"
  lazy val delta = "io.delta" %% "delta-core" % "1.0.0"
  lazy val mainargs = "com.lihaoyi" %% "mainargs" % "0.2.1"

  // Hadoop Dependencies
  lazy val HadoopVer = "3.2.0"
  lazy val AwsSdkVer =
    "1.11.375" //aws sdk must match to Hadoop<->Aws transitive dependency version

  lazy val hadoopCommon = "org.apache.hadoop" % "hadoop-common" % HadoopVer
  lazy val hadoopAws = ("org.apache.hadoop" % "hadoop-aws" % HadoopVer)
    .exclude("com.amazonaws", "aws-java-sdk-bundle")
  lazy val awsS3Sdk = "com.amazonaws" % "aws-java-sdk-s3" % AwsSdkVer
  lazy val awsDynmodbSdk =
    "com.amazonaws" % "aws-java-sdk-dynamodb" % AwsSdkVer    
}
