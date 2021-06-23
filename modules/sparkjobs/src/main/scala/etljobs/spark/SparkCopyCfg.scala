package etljobs.spark

import mainargs.{main, arg, Flag, TokensReader, ParserForClass}
import etljobs.common.FileCopyCfg
import FileFormat._

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
case class SparkOption(name: String, value: String)

@main
case class SparkCopyCfg(
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
    @arg(
      name = "move-files",
      doc =
        "Whether to move files to processed directory inside the job context"
    )
    moveFiles: Flag,
    @arg(
      name = "reader-options",
      doc = "<name>:<value> list of options to be passed to Spark reader"
    )
    readerOptions: Option[List[SparkOption]],
    fileCopy: FileCopyCfg
)

object SparkCopyCfg {
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
  implicit object SparkOptionRead
      extends TokensReader[SparkOption](
        "input file or output file/table format",
        strs =>
          strs.headOption.map(_.split(":").toList) match {
            case Some(l) =>
              l match {
                case name :: value :: _ => Right(SparkOption(name, value))
                case _ =>
                  Left(
                    "Reader option must have name and value separated by colon ':', example '<name>:<value>'"
                  )
              }
            case _ => Left("There must be at least one reader option")
          }
      )

  implicit def copyParamsParser = ParserForClass[SparkCopyCfg]
}
