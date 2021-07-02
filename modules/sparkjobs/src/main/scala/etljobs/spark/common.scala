package etljobs.spark

import etljobs.common.FsUtil._
import etljobs.common.{EntityPattern, FileCopyCfg, SparkOption}

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import scala.io.Source
import java.net.URI
import java.time.format.DateTimeFormatter
import java.time.LocalDate

object common {
  val SparkOptions = Map(
    "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.databricks.delta.schema.autoMerge.enabled" -> "true"
  )

  val DateFormatter = DateTimeFormatter.ofPattern("YYYY-MM-dd")

  def dateLit(date: LocalDate) =
    lit(date.format(DateFormatter))

  def sparkWithConfig(
      hadoopConfig: List[SparkOption]
  ): SparkSession.Builder = {
    val hadoop = hadoopConfig.map(c => c.name -> c.value).toMap
    (hadoop ++ SparkOptions).foldLeft(SparkSession.builder) {
      case (acc, (name, value)) =>
        acc.config(name, value)
    }
  }

  def useResource[T <: AutoCloseable, U](r: T)(f: T => U): U =
    try f(r)
    finally r.close()

  def requireMove(cfg: SparkCopyCfg): Boolean =
    cfg.moveFiles.value || cfg.fileCopy.processedDir.isDefined

  def moveFiles(
      conf: Configuration,
      entityPatterns: List[EntityPattern],
      processedDir: Option[URI],
      input: URI
  ): Unit = {
    val dest =
      processedDir.getOrElse(new URI(s"$input/processed"))
    entityPatterns.foreach { p =>
      val srcFiles =
        listFiles(conf, p.globPattern, input)
      println(s"moving files: ${srcFiles.mkString(",\n")}\nto $dest")
      srcFiles.foreach(src => moveFile(src, dest, conf))
    }
  }

  def readSchema(
      conf: Configuration,
      schemaPath: URI,
      entityName: String
  ): StructType = {
    val fs = FileSystem.get(conf)
    val in = fs.open(new Path(s"$schemaPath/$entityName.json"))
    val jsonSchema = useResource(in) { stream =>
      Source
        .fromInputStream(stream)
        .getLines
        .mkString
    }
    DataType.fromJson(jsonSchema).asInstanceOf[StructType]
  }

  def getInPath(fileCopy: FileCopyCfg): URI = {
    val context =
      JobContext(
        fileCopy.ctx.dagId,
        fileCopy.ctx.executionDate
      )
    contextDir(fileCopy.inputPath, context)
  }

  def getInOutPaths(fileCopy: FileCopyCfg): (URI, URI) = {
    val output = new URI(s"${fileCopy.outputPath}/${fileCopy.ctx.dagId}")
    val input = getInPath(fileCopy)
    (input, output)
  }
}
