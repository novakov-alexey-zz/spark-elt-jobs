package etljobs.spark

import etljobs.common.FsUtil._
import etljobs.common.{EntityPattern, FileCopyCfg, SparkOption}

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession

import scala.io.Source
import java.net.URI

object common {

  val SparkOptions = Map(
    "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.databricks.delta.schema.autoMerge.enabled" -> "true"
  )

  def sparkWithConfig(
      hadoopConfig: List[SparkOption]
  ): SparkSession.Builder = {
    val hadoop = hadoopConfig.map(c => c.name -> c.value).toMap
    (hadoop ++ SparkOptions).foldLeft(SparkSession.builder) {
      case (acc, (name, value)) =>
        acc.config(name, value)
    }
  }

  def useResource[T <: AutoCloseable](r: T)(f: T => Unit) =
    try f(r)
    finally r.close()

  def requireMove(cfg: SparkCopyCfg) =
    cfg.moveFiles.value || cfg.fileCopy.processedDir.isDefined

  def moveFiles(
      conf: Configuration,
      entityPatterns: List[EntityPattern],
      processedDir: Option[URI],
      input: URI
  ) = {
    val dest =
      processedDir.getOrElse(input.resolve("processed"))
    entityPatterns.foreach { p =>
      val srcFiles =
        listFiles(conf, p.globPattern, input)
      println(s"moving files: ${srcFiles.mkString(",\n")} to $dest")
      srcFiles.foreach(src => moveFile(src, dest, conf))
    }
  }

  def getSchema(schemaPath: URI, entityName: String): StructType = {
    val jsonSchema =
      Source
        .fromFile(schemaPath.resolve(s"$entityName.json"))
        .getLines
        .mkString
    DataType.fromJson(jsonSchema).asInstanceOf[StructType]
  }

  def getInOutPaths(fileCopy: FileCopyCfg) = {
    val context =
      JobContext(
        fileCopy.dagId,
        fileCopy.executionDate
      )
    val output =
      fileCopy.outputPath.resolve(fileCopy.dagId)

    val input = contextDir(fileCopy.inputPath, context)
    (input, output)
  }
}
