package example

import java.time.LocalDate
import java.io.File
import java.nio.file.Path

import scala.util.Try
import FsUtil._

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path => HPath}
import org.apache.hadoop.fs.GlobFilter

import mainargs.{
  main,
  arg,
  ParserForMethods,
  ParserForClass,
  Flag,
  TokensReader
}

object Params {
  implicit object PathRead
      extends TokensReader[Path](
        "path",
        strs => Right(Path.of(strs.head))
      )
  implicit object DateRead
      extends TokensReader[LocalDate](
        "executionDate",
        strs => Right(LocalDate.parse(strs.head))
      )

  implicit def paramsParser = ParserForClass[Params]
}

@main
case class Params(
    @arg(short = 'i', doc = "Path to input directory")
    inputPath: Path,
    @arg(short = 'o', doc = "Output directory")
    outputPath: Path,
    @arg(
      name = "execution-date",
      doc = "job execution date to choose file name with. Format YYYY-MM-DD, example 2000-01-01"
    )
    executionDate: LocalDate,
    @arg(short = 'd', doc = "DAG id to create sub-folder inside the outputPath")
    dagId: String,
    @arg(
      short = 't',
      doc = "Task id to create sub-folder inside the outputPath"
    )
    taskId: String,
    @arg(
      name = "glob-pattern",
      doc = "Filter inputPath based on glob pattern"
    )
    globPattern: String
)

object FsUtil {
  def hadoopCopy(inputDir: Path, globPattern: String, outputDir: String) = {
    val hadoopConfig = new Configuration()
    val fs = FileSystem.get(hadoopConfig)
    val filter = new GlobFilter(globPattern)

    val srcFiles = FileUtil
      .listFiles(inputDir.toFile())
      .filter(f => filter.accept(new HPath(f.toString())))

    srcFiles.foreach { src =>
      val destPath = new HPath(outputDir + src.getName())
      FileUtil.copy(src, fs, destPath, true, hadoopConfig)
    }
  }

  def sparkCopy(inputDir: Path, globPattern: String, outputDir: String) = {
    val spark =
      SparkSession.builder
        .appName("File2Location Application")
        .master("local")
        .getOrCreate()

    val inputData =
      spark.read
        .format("csv")
        .option("pathGlobFilter", globPattern)
        .load(inputDir.toString())

    inputData.write.option("path", outputDir)
    spark.stop()
  }

  def outputDir(params: Params) =
    s"${params.outputPath}/${params.dagId}/${params.taskId}/${params.executionDate.toString}/"
}

object FileToFile extends App {
  @main
  def run(params: Params) =
    FsUtil.hadoopCopy(params.inputPath, params.globPattern, outputDir(params))

  ParserForMethods(this).runOrExit(args)
}

object FileToDataset extends App {
  @main
  def run(params: Params) =
    FsUtil.sparkCopy(params.inputPath, params.globPattern, outputDir(params))

  ParserForMethods(this).runOrExit(args)
}
