package etljobs.hadoop

import mainargs.{main, ParserForMethods, ParserForClass, arg}
import etljobs.common.MainArgsUtil._
import etljobs.common.FsUtil.JobContext
import etljobs.common.{FsUtil, SparkOption, HadoopCfg}

import java.time.LocalDate
import java.net.URI
import java.nio.file.Path

case class CheckCfg(
    @arg(short = 'i', doc = "Path to input directory")
    inputPath: URI,
    @arg(
      name = "execution-date",
      doc =
        "job execution date to choose file name with. Format YYYY-MM-DD, example 2000-01-01"
    )
    executionDate: LocalDate,
    @arg(short = 'd', doc = "DAG id to create sub-folder inside the outputPath")
    dagId: String,
    @arg(
      name = "glob-pattern",
      doc = "Filter inputPath based on glob pattern"
    )
    globPattern: String,
    @arg(
      name = "file-prefixes",
      doc =
        "file prefixes to check if they exist in 'inputPath' for a specific 'execution-date'"
    )
    filePrefixes: List[String],
    @arg(
      name = "hadoop-config",
      doc =
        "<name>:<value> list of options to be passed to hadoop configuration"
    )
    hadoopConfig: List[SparkOption]
)

object CheckCfg {
  implicit def cfgParser = ParserForClass[CheckCfg]
}

object CheckFileExists extends App {
  val FilesExistCode = 0
  val FilesAbsentCode = 99

  @main
  def run(cfg: CheckCfg) = {
    val targetPath = FsUtil.contextDir(
      cfg.inputPath,
      JobContext(cfg.dagId, cfg.executionDate)
    )
    val conf = HadoopCfg.get(cfg.hadoopConfig)
    val inputFiles = FsUtil.listFiles(conf, cfg.globPattern, targetPath)
    println(s"input files:\n${inputFiles.mkString("\n")}")
    val inputNames =
      inputFiles.map(uri => Path.of(uri.toString()).getFileName().toString())
    val filesExist =
      cfg.filePrefixes.forall(p => inputNames.exists(_.startsWith(p)))
    println(s"all file exist: $filesExist")
    val ec = if (filesExist) FilesExistCode else FilesAbsentCode
    System.exit(ec)
  }

  ParserForMethods(this).runOrExit(args)
}
