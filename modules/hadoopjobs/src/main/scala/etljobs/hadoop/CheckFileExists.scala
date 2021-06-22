package etljobs.hadoop

import mainargs.{main, ParserForMethods, ParserForClass, arg}
import java.nio.file.Path
import etljobs.common.MainArgsUtil._
import java.time.LocalDate
import etljobs.common.FsUtil.JobContext
import etljobs.common.FsUtil

case class Params(
    @arg(short = 'i', doc = "Path to input directory")
    inputPath: Path,
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
    filePrefixes: List[String]
)

object Params {
  implicit def paramsParser = ParserForClass[Params]
}

object CheckFileExists extends App {
  val FilesExistCode = 0
  val FilesAbsentCode = 99

  @main
  def run(params: Params) = {
    val targetPath = FsUtil.contextDir(
      params.inputPath,
      JobContext(params.dagId, params.executionDate)
    )
    val inputFiles = FsUtil.listFiles(params.globPattern, targetPath)
    println(s"input files: ${inputFiles.mkString("\n")}")
    val inputNames = inputFiles.map(_.getName())
    val filesExist =
      params.filePrefixes.forall(p => inputNames.exists(_.startsWith(p)))
    println(s"all file exist: $filesExist")
    val ec = if (filesExist) FilesExistCode else FilesAbsentCode
    System.exit(ec)
  }

  ParserForMethods(this).runOrExit(args)
}
