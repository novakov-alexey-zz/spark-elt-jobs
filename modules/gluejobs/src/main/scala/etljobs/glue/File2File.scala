package etljobs.glue

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{DataType, IntegerType, StructType}
import org.apache.spark.sql.{Column, SparkSession}

import java.net.URI
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.io.Source

object HadoopCfg {
  def get(options: List[SparkOption]): Configuration =
    options.foldLeft(new Configuration) { case (acc, o) =>
      acc.set(o.name, o.value)
      acc
    }
}

object File2File {
  val DatePattern = "yyyy-MM-dd"
  val DateFormatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern(DatePattern)

  def dateLit(date: LocalDate): Column =
    to_date(lit(date.format(DateFormatter)), DatePattern)

  def useResource[T <: AutoCloseable, U](r: T)(f: T => U): U =
    try f(r)
    finally r.close()

  def getTrigger(interval: Long): Trigger =
    if (interval < 0) Trigger.Once
    else Trigger.ProcessingTime(interval)

  def readSchema(
      conf: Configuration,
      schemaPath: URI,
      entityName: String
  ): StructType = {
    val fs = FileSystem.get(schemaPath, conf)
    val in = fs.open(new Path(s"$schemaPath/$entityName.json"))
    val jsonSchema = useResource(in) { stream =>
      Source
        .fromInputStream(stream)
        .getLines
        .mkString
    }
    DataType.fromJson(jsonSchema).asInstanceOf[StructType]
  }

  def run(session: SparkSession, cfg: JobConfig): Unit = {
    val conf = HadoopCfg.get(cfg.hadoopConfig)
    val trigger = getTrigger(cfg.triggerInterval)
    val input = new URI(
      s"${cfg.inputPath}/${cfg.ctx.jobId}/${cfg.ctx.executionDate.toString}"
    )
    val output = new URI(s"${cfg.outputPath}/${cfg.ctx.jobId}")

    val queries = cfg.entityPatterns.map { entity =>
      val stream = cfg.readerOptions
        .foldLeft(session.readStream) { (acc, o) =>
          acc.option(o.name, o.value)
        }
      val schema = readSchema(conf, cfg.schemaPath, entity.name)
      val outputPath = new URI(s"$output/${entity.name}")
      val checkpointPath = new URI(s"$outputPath/_checkpoints")
      val executionDateCol = dateLit(cfg.ctx.executionDate)
      stream
        .format(cfg.inputFormat)
        .schema(schema)
        .load(s"$input/${entity.globPattern}")
        .withColumn("year", year(executionDateCol).cast(IntegerType))
        .withColumn("month", month(executionDateCol).cast(IntegerType))
        .withColumn("day", dayofmonth(executionDateCol).cast(IntegerType))
        .writeStream
        .outputMode(OutputMode.Append)
        .option("checkpointLocation", checkpointPath.toString)
        .partitionBy(cfg.partitionBy: _*)
        .trigger(trigger)
        .format(cfg.saveFormat)
        .start(outputPath.toString)
    }

    println(s"waiting for termination")
    queries.foreach(_.awaitTermination)
    println(s"all ${queries.length} streaming queries are terminated")

    if (cfg.moveFiles)
      FsUtil.moveFiles(
        conf,
        cfg.entityPatterns,
        cfg.processedDir,
        input
      )
  }
}
