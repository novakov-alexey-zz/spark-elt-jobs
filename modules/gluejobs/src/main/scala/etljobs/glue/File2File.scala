package etljobs.glue

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{Column, SparkSession}

import java.net.URI
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.io.Source

case class EntityPattern(
    name: String,
    globPattern: String,
    dedupKey: Option[String]
)

case class SparkOption(name: String, value: String)

case class JobContext(executionDate: LocalDate, jobId: String)

case class JobConfig(
    ctx: JobContext,
    triggerInterval: Long, // negative value means Trigger.Once
    inputFormat: String,
    saveFormat: String,
    inputPath: URI,
    outputPath: URI,
    hadoopConfig: List[SparkOption],
    readerOptions: List[SparkOption],
    schemaPath: URI,
    partitionBy: List[String],
    moveFiles: Boolean,
    entityPatterns: List[EntityPattern],
    processedDir: Option[URI] = None
)

object HadoopCfg {
  def get(options: List[SparkOption]): Configuration =
    options.foldLeft(new Configuration) { case (acc, o) =>
      acc.set(o.name, o.value)
      acc
    }
}

object FsUtil {

  private def listFiles(
      conf: Configuration,
      globPattern: String,
      inputPath: URI
  ): Array[URI] = {
    val fs = FileSystem.get(inputPath, conf)
    val path = new Path(s"$inputPath/$globPattern")
    val statuses = fs.globStatus(path)
    statuses.map(_.getPath.toUri())
  }

  private def moveFile(
      src: URI,
      destinationDir: URI,
      conf: Configuration
  ): Boolean = {
    val fileName = src.toString
      .split("/")
      .lastOption
      .getOrElse(
        sys.error(s"Failed to get file name from the path: $src")
      )
    val destPath = new Path(s"$destinationDir/$fileName")
    val srcFs = FileSystem.get(src, conf)
    val srcPath = new Path(src.toString)
    val destFs = FileSystem.get(destPath.toUri, conf)

    FileUtil.copy(
      srcFs,
      srcPath,
      destFs,
      destPath,
      true,
      conf
    )
  }

  def moveFiles(
      conf: Configuration,
      entityPatterns: List[EntityPattern],
      processedDir: Option[URI],
      input: URI
  ): Unit = {
    val dest =
      processedDir.getOrElse(new URI(s"$input/processed"))
    entityPatterns.foreach { p =>
      val srcFiles = listFiles(conf, p.globPattern, input)
      println(s"moving files: ${srcFiles.mkString(",\n")}\nto $dest")
      srcFiles.foreach(src => moveFile(src, dest, conf))
    }
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
        .withColumn("year", year(executionDateCol))
        .withColumn("month", month(executionDateCol))
        .withColumn("day", dayofmonth(executionDateCol))
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
