import etljobs.glue._

import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import org.apache.spark.SparkContext

import scala.collection.JavaConverters._
import java.net.URI
import java.time.LocalDate

object FileToFile extends App {
  val sc = new SparkContext()
  val gc = new GlueContext(sc)
  val session = gc.getSparkSession
  val glueArgs = GlueArgParser.getResolvedOptions(args, Seq("JOB_NAME").toArray)
  val jobName = glueArgs("JOB_NAME")
  Job.init(jobName, gc, glueArgs.asJava)

  val cfg = JobConfig(
    JobContext(LocalDate.of(2021, 6, 21), jobName),
    triggerInterval = -1,
    "csv",
    "parquet",
    new URI("s3a://input-data-etl"),
    new URI("s3a://raw-data-etl"),
    List(
      SparkOption("fs.s3a.endpoint", "s3.eu-central-1.amazonaws.com"),
      SparkOption(
        "fs.s3a.assumed.role.arn",
        "arn:aws:iam::339364330848:role/Lambda2S3Bucket"
      ),
      SparkOption("fs.s3a.connection.ssl.enabled", "true")
    ),
    List(SparkOption("header", "true")),
    new URI(s"s3a://input-data-etl/$jobName/schema"),
    partitionBy = "date",
    moveFiles = true,
    List(
      EntityPattern("items", "items_*2021-06-21.csv", Some("itemId")),
      EntityPattern("orders", "orders_*2021-06-21.csv", Some("orderId")),
      EntityPattern(
        "customers",
        "customers_*2021-06-21.csv",
        Some("customerId")
      )
    )
  )
  File2File.run(session, cfg)
  Job.commit()
}
