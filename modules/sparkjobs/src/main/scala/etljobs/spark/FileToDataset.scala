package etljobs.spark

import DataFormat._
import common._
import etljobs.common.EntityPattern

import mainargs.{main, ParserForMethods}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame
import io.delta.tables._

import java.nio.file.Path

object FileToDataset extends App {
  @main
  def run(params: SparkCopyCfg) =
    sparkCopy(params)

  private def toDeltaTable(
      targetPath: String,
      targetSchema: Path,
      entity: EntityPattern,
      spark: SparkSession,
      key: String,
      inputDF: DataFrame
  ) = {
    val schema = getSchema(
      targetSchema,
      entity.name
    )
    val target =
      DeltaTable
        .createIfNotExists(spark)
        .location(targetPath)
        .addColumns(schema)
        .execute()
        .as("target")
    target
      .merge(inputDF.as("updates"), s"target.$key = updates.$key")
      .whenNotMatched()
      .insertAll()
      .execute()
  }

  private def loadFileToSpark(
      entity: EntityPattern,
      spark: SparkSession,
      cfg: SparkCopyCfg,
      input: Path,
      output: Path,
      saveMode: SaveMode
  ) = {
    val inputDataReader =
      spark.read
        .option("pathGlobFilter", entity.globPattern)
        .option("inferSchema", "true")
        .format(cfg.inputFormat.toSparkFormat)
    val inputDataWithOptions =
      cfg.readerOptions.getOrElse(List.empty).foldLeft(inputDataReader) {
        case (acc, opt) => acc.option(opt.name, opt.value)
      }

    val inputDF = inputDataWithOptions.load(input.toString())

    lazy val inputDataToWrite =
      inputDF.write.mode(saveMode)

    val writer = cfg.saveFormat match {
      case CSV     => inputDataToWrite.csv _
      case JSON    => inputDataToWrite.json _
      case Parquet => inputDataToWrite.parquet _
      case Delta =>
        tablePath: String =>
          entity.dedupKey match {
            case Some(key) =>
              val schemaPath = cfg.schemaPath.getOrElse(
                sys.error(
                  s"struct schema for Delta table $tablePath is required"
                )
              )
              toDeltaTable(tablePath, schemaPath, entity, spark, key, inputDF)
            case None =>
              inputDataToWrite.save(tablePath)
          }
    }
    writer(output.toString())
  }

  def sparkCopy(cfg: SparkCopyCfg) = {
    val (input, output) = getInOutPaths(cfg.fileCopy)
    println(s"input path: $input")

    val sparkSession = SparkSession.builder
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      )
      .getOrCreate()

    useResource(sparkSession) { spark =>
      lazy val saveMode = getSaveMode(cfg.fileCopy.overwrite.value)
      cfg.fileCopy.entityPatterns.foreach { p =>
        val entityOutPath = output.resolve(p.name)
        println(s"output path: $entityOutPath")
        loadFileToSpark(p, spark, cfg, input, entityOutPath, saveMode)
      }

      if (requireMove(cfg))
        moveFiles(
          cfg.fileCopy.entityPatterns,
          cfg.fileCopy.processedDir,
          input
        )
    }
  }

  private def getSaveMode(overwrite: Boolean) =
    overwrite match {
      case true => SaveMode.Overwrite
      case _    => SaveMode.ErrorIfExists
    }

  ParserForMethods(this).runOrExit(args)
}
