package etljobs.common

import MainArgsUtil._

import org.apache.hadoop.conf.Configuration
import mainargs.{main, arg, ParserForClass, TokensReader, Flag}

import java.time.LocalDate
import java.net.URI

object HadoopCfg {
  def get(options: List[SparkOption]) =
    options.foldLeft(new Configuration) { case (acc, o) =>
      acc.set(o.name, o.value)
      acc
    }
}

@main
case class SparkOption(name: String, value: String)

object SparkOption {
  val NameValueSeparator = ":"

  implicit object SparkOptionRead
      extends TokensReader[SparkOption](
        "input file or output file/table format",
        strs =>
          strs.headOption.flatMap { s =>
            val i = s.indexOf(NameValueSeparator)
            if (i > 0) {
              val (name, value) = s.splitAt(i)
              Some((name, value.stripPrefix(NameValueSeparator)))
            } else None
          } match {
            case Some((name, value)) => Right(SparkOption(name, value))
            case _                   => Left("There must be at least one reader option")
          }
      )
}

@main
case class ContextCfg(
    @arg(
      name = "execution-date",
      doc =
        "job execution date to choose file name with. Format YYYY-MM-DD, example 2000-01-01"
    )
    executionDate: LocalDate,
    @arg(short = 'j', doc = "Job id to create sub-folder inside the outputPath")
    jobId: String
)

object ContextCfg {
  implicit def contextCfgParser = ParserForClass[ContextCfg]
}

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
    inputPath: URI,
    @arg(short = 'o', doc = "Output directory")
    outputPath: URI,
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
    processedDir: Option[URI],
    @arg(
      doc = "Ovewrite destination files if they exist"
    )
    overwrite: Flag,
    @arg(
      name = "hadoop-config",
      doc =
        "<name>:<value> list of options to be passed to hadoop configuration"
    )
    hadoopConfig: List[SparkOption],
    ctx: ContextCfg
)
