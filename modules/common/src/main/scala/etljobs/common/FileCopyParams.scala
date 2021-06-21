package etljobs.common

import mainargs.{main, arg, ParserForClass, TokensReader, Flag}
import java.time.LocalDate
import java.nio.file.Path
import MainArgsUtil._

object FileCopyParams {
  implicit object EntityPatternRead
      extends TokensReader[EntityPattern](
        "entityPattern",
        strs => {
          assert(strs.nonEmpty, "At least one entityPattern must be specified")
          val pair = strs.head.split(":")
          assert(
            strs.head.contains(":"),
            "entityPattern must contain semicolon as separator of the entity name and pattern"
          )
          Right(EntityPattern(pair(0), pair(1)))
        }
      )

  implicit def paramsParser = ParserForClass[FileCopyParams]
}

@main
case class EntityPattern(name: String, globPattern: String)

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
      name = "glob-pattern",
      short = 'p',
      doc =
        "Entity name and and GLOB pattern for 'inputPath' in format <name:pattern>"
    )
    entityPatterns: List[EntityPattern],
    @arg(
      name = "processed-dir",
      doc = "A path to move processed source files into"
    )
    processedDir: Option[Path],
    @arg(
      doc = "Ovewrite destination files if they exist"
    )
    overwrite: Flag
)
