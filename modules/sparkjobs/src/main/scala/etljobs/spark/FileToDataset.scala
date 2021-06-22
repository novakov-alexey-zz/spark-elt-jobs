package etljobs.spark

import mainargs.{main, ParserForMethods}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import FileFormat._
import common._
import java.nio.file.Path
import etljobs.common.FsUtil._
import etljobs.common.EntityPattern

object FileToDataset extends App {
  @main
  def run(params: SparkCopyCfg) =
    sparkCopy(params)

  private def loadFileToSpark(
      pattern: String,
      spark: SparkSession,
      params: SparkCopyCfg,
      input: Path,
      output: Path,
      saveMode: SaveMode
  ) = {
    val inputData =
      spark.read
        .option("pathGlobFilter", pattern)
        .option("header", "true") //TODO: extract to CSV config
        .option("inferSchema", "true")
        .format(params.inputFormat.toSparkFormat)
        .load(input.toString())
    val inputDataToWrite = inputData.write
      .mode(saveMode)

    val writer = params.saveFormat match {
      case CSV     => inputDataToWrite.csv _
      case JSON    => inputDataToWrite.json _
      case Parquet => inputDataToWrite.parquet _
    }
    writer(output.toString())
  }

  def sparkCopy(cfg: SparkCopyCfg) = {
    val context =
      JobContext(cfg.fileCopy.dagId, cfg.fileCopy.executionDate)
    val output = contextDir(cfg.fileCopy.outputPath, context)

    val input = contextDir(cfg.fileCopy.inputPath, context)
    println(s"input path: $input")

    val sparkSession = SparkSession.builder.getOrCreate()
    useResource(sparkSession) { spark =>
      lazy val saveMode = getSaveMode(cfg.fileCopy.overwrite.value)
      cfg.fileCopy.entityPatterns.foreach { p =>
        val out = output.resolve(p.name)
        println(s"output path: $out")
        loadFileToSpark(p.globPattern, spark, cfg, input, out, saveMode)
      }

      // move source files to processed directory
      if (cfg.moveFiles.value || cfg.fileCopy.processedDir.isDefined) {
        moveFiles(
          cfg.fileCopy.entityPatterns,
          cfg.fileCopy.processedDir,
          input
        )
      }
    }
  }

  private def moveFiles(
      entityPatterns: List[EntityPattern],
      processedDir: Option[Path],
      input: Path
  ) = {
    val dest =
      processedDir.getOrElse(input.resolve("processed"))
    val fs = FileSystem.get(new Configuration())
    entityPatterns.foreach { p =>
      val srcFiles =
        listFiles(p.globPattern, input)
      println(s"moving files: ${srcFiles.mkString(",")} to $dest")
      srcFiles.foreach(src => moveFile(src, dest, fs))
    }
  }

  private def getSaveMode(overwrite: Boolean) =
    overwrite match {
      case true => SaveMode.Overwrite
      case _    => SaveMode.ErrorIfExists
    }

  ParserForMethods(this).runOrExit(args)
}
