package etljobs.common

import mainargs.{main, arg, ParserForClass, TokensReader, Flag}
import java.time.LocalDate
import java.nio.file.Path
import MainArgsUtil._

object FileCopyCfg {
  implicit object EntityPatternRead
      extends TokensReader[EntityPattern](
        "entityPattern",
        strs => {
          assert(strs.nonEmpty, "At least one entityPattern must be specified")
          assert(
            strs.head.contains(":"),
            "entityPattern must contain semicolon as separator of the entity name and pattern"
          )
          val entity = strs.head.split(":").toList
          entity match {
            case name :: pattern :: maybeDedupKey =>
              Right(EntityPattern(name, pattern, maybeDedupKey.headOption))
            case _ =>
              Left(s"entity pattern does not contain name and glob pattern")
          }
        }
      )

  implicit def cfgParser = ParserForClass[FileCopyCfg]
}

@main
case class EntityPattern(
    name: String,
    globPattern: String,
    dedupKey: Option[String]
)

@main
case class FileCopyCfg(
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
      name = "entity-pattern",
      short = 'p',
      doc =
        "Entity name, GLOB pattern and dedupKey for 'inputPath' in format <name:pattern[:dedupKey]>"
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
