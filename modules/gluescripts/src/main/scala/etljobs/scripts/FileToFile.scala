package etljobs.scripts

import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.log.GlueLogger
import com.amazonaws.services.glue.util.{GlueArgParser, Job}
import etljobs.glue._
import org.apache.spark.SparkContext

import java.net.URI
import java.time.LocalDate
import scala.collection.JavaConverters._

object FileToFile extends App {
  val sc = new SparkContext()
  val gc = new GlueContext(sc)
  val session = gc.getSparkSession
  val glueArgs = GlueArgParser.getResolvedOptions(
    args,
    Seq(
      "JOB_NAME",
      "fs.s3a.endpoint",
      "iam-role",
      "input-bucket",
      "output-bucket",
      "schema-bucket"
    ).toArray
  )
  val logger = new GlueLogger
  logger.info(s"glueArgs: $glueArgs")
  val jobName = glueArgs("JOB_NAME")
  Job.init(jobName, gc, glueArgs.asJava)

  val hadoopConfig = List(
    SparkOption("fs.s3a.endpoint", glueArgs("fs.s3a.endpoint")),
    SparkOption("fs.s3a.assumed.role.arn", glueArgs("iam_role")),
    SparkOption("fs.s3a.connection.ssl.enabled", "true")
  )
  val execDate = "2021-06-21"
  val cfg = JobConfig(
    JobContext(LocalDate.parse(execDate), jobName),
    triggerInterval = -1,
    "csv",
    "parquet",
    new URI(glueArgs("input_bucket")),
    new URI(glueArgs("output_bucket")),
    hadoopConfig,
    List(SparkOption("header", "true")),
    new URI(glueArgs("schema_bucket")),
    partitionBy = List("year", "month", "day"),
    moveFiles = false,
    List(
      EntityPattern("items", s"items_*$execDate.csv", Some("itemId")),
      EntityPattern("orders", s"orders_*$execDate.csv", Some("orderId")),
      EntityPattern(
        "customers",
        s"customers_*$execDate.csv",
        Some("customerId")
      )
    )
  )
  logger.info(s"Staring spark File2File streaming job with configuration: $cfg")

  File2File.run(session, cfg)
  Job.commit()
}
