package etljobs.spark

import mainargs.{main, arg, Flag, TokensReader, ParserForClass}
import etljobs.common.FileCopyCfg
import etljobs.common.MainArgsUtil.PathRead
import DataFormat._
import java.nio.file.Path

sealed trait DataFormat {
  def toSparkFormat: String =
    getClass.getSimpleName.toLowerCase.stripSuffix("$")
}

object DataFormat {
  case object CSV extends DataFormat
  case object JSON extends DataFormat
  case object Parquet extends DataFormat
  case object Delta extends DataFormat
}

@main
case class SparkOption(name: String, value: String)

@main
case class SparkCopyCfg(
    @arg(
      name = "input-format",
      doc = "Data input format to be used by Spark Datasource API on read"
    )
    inputFormat: DataFormat,
    @arg(
      name = "output-format",
      doc = "Data ouput format to be used by Spark Datasource API on write"
    )
    saveFormat: DataFormat,
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
    @arg(
      short = 's',
      name = "schema-path",
      doc = "A path to schema directory for all entities as per entityPatterns"
    )
    schemaPath: Option[Path],
    @arg(
      name = "partition-by",
      doc = "Table column to parition by"
    )
    partitionBy: String,
    fileCopy: FileCopyCfg
)

object SparkCopyCfg {
  implicit object DataFormatRead
      extends TokensReader[DataFormat](
        "input file or output file/table format",
        strs =>
          strs.head match {
            case "csv"     => Right(CSV)
            case "json"    => Right(JSON)
            case "parquet" => Right(Parquet)
            case "delta"   => Right(Delta)
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
