package etljobs.spark

import mainargs.{main, ParserForMethods, ParserForClass, TokensReader, arg}
import etljobs.common.FsUtil._
import etljobs.common.FileCopyParams
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import FileFormat._
import java.nio.file.Path

sealed trait FileFormat {
  def toSparkFormat: String =
    getClass.getSimpleName.toLowerCase.stripSuffix("$")
}

object FileFormat {
  case object CSV extends FileFormat
  case object JSON extends FileFormat
  case object Parquet extends FileFormat
}

@main
case class SparkCopyParams(
    @arg(
      name = "input-format",
      doc = "File input format to be used by Spark Datasource API on read"
    )
    inputFormat: FileFormat,
    @arg(
      name = "output-format",
      doc = "File ouput format to be used by Spark Datasource API on write"
    )
    saveFormat: FileFormat,
    copyParams: FileCopyParams
)

object SparkCopyParams {
  implicit object FileFormatRead
      extends TokensReader[FileFormat](
        "input file or output file/table format",
        strs =>
          strs.head match {
            case "csv"     => Right(CSV)
            case "json"    => Right(JSON)
            case "parquet" => Right(Parquet)
            case _         => Left("Unknown file format")
          }
      )

  implicit def copyParamsParser = ParserForClass[SparkCopyParams]
}

object FileToDataset extends App {
  @main
  def run(params: SparkCopyParams) =
    sparkCopy(params)

  def loadFileToSpark(
      pattern: String,
      spark: SparkSession,
      params: SparkCopyParams,
      input: Path,
      output: Path
  ) = {
    val inputData =
      spark.read
        .option("pathGlobFilter", pattern)
        .option("header", "true") //TODO: extract to CSV config
        .option("inferSchema", "true")
        .format(params.inputFormat.toSparkFormat)
        .load(input.toString())
    val inputDataToWrite = inputData.write
      .mode(SaveMode.Overwrite)
    val out = output.toString()

    params.saveFormat match {
      case CSV     => inputDataToWrite.csv(out)
      case JSON    => inputDataToWrite.json(out)
      case Parquet => inputDataToWrite.parquet(out)
    }
  }

  def sparkCopy(params: SparkCopyParams) = {
    val context =
      JobContext(params.copyParams.dagId, params.copyParams.executionDate)
    val output = targetDir(params.copyParams.outputPath, context)

    val input = targetDir(params.copyParams.inputPath, context)
    println(s"input path: $input")

    val sparkSession = SparkSession.builder.getOrCreate()
    useResource(sparkSession) { spark =>
      params.copyParams.entityPatterns.foreach { p =>
        val out = output.resolve(p.name)
        println(s"output path: $out")
        loadFileToSpark(p.globPattern, spark, params, input, out)
      }

      // move source files to processed directory
      if (params.copyParams.processedDir.isDefined) {
        val fs = FileSystem.get(new Configuration())
        params.copyParams.entityPatterns.foreach { p =>
          val srcFiles =
            listFiles(p.globPattern, params.copyParams.inputPath)
          params.copyParams.processedDir.foreach { dst =>
            srcFiles.foreach { src =>
              moveFile(src, dst, fs)
            }
          }
        }
      }
    }
  }

  def useResource[T <: AutoCloseable](r: T)(f: T => Unit) =
    try f(r)
    finally r.close()

  ParserForMethods(this).runOrExit(args)
}
