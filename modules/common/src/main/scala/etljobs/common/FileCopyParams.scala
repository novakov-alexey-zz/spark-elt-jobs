package etljobs.common

import mainargs.{main, arg, ParserForClass}
import java.time.LocalDate
import java.nio.file.Path
import mainargs.Flag
import MainArgsUtil._

object FileCopyParams {
  implicit def paramsParser = ParserForClass[FileCopyParams]
}

@main
case class FileCopyParams(
    @arg(short = 'i', doc = "Path to input directory")
    inputPath: Path,
    @arg(short = 'o', doc = "Output directory")
    outputPath: Path,
    @arg(
      name = "execution-date",
      doc =
        "job execution date to choose file name with. Format YYYY-MM-DD, example 2000-01-01"
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
    globPattern: String,
    @arg(
      name = "move-sources",
      doc = "Move source files to processedDir"
    )
    moveSourceFiles: Flag,
    @arg(
      name = "processed-dir",
      doc = "A path to move processed source files into"
    )
    processedDir: Path,
    @arg(
      doc = "Ovewrite destination files if they exist"
    )
    overwrite: Flag
)
