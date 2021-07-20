package etljobs.scripts

import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.{GlueArgParser, Job}
import etljobs.glue.HudiDemo
import org.apache.spark.{SparkConf, SparkContext}

import java.net.URI
import scala.collection.JavaConverters._

object HudiScript extends App {
  val conf = new SparkConf()
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.sql.hive.convertMetastoreParquet", "false")
  val sc = new SparkContext(conf)
  val gc = new GlueContext(sc)
  val session = gc.getSparkSession
  val glueArgs = GlueArgParser.getResolvedOptions(
    args,
    Seq("JOB_NAME").toArray
  )
  val jobName = glueArgs("JOB_NAME")
  Job.init(jobName, gc, glueArgs.asJava)

  val outputPath = new URI("s3a://raw-data-etl/hudi-dataset")
  HudiDemo.run(session, outputPath)
  Job.commit()
}
