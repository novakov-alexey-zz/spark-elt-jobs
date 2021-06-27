package etljobs.spark

import etljobs.common.{FileCopyCfg, SparkOption}
import etljobs.common.MainArgsUtil.UriRead
import DataFormat._

import mainargs.{main, arg, Flag, TokensReader, ParserForClass}

import java.net.URI

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
    schemaPath: Option[URI],
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

  implicit def copyParamsParser = ParserForClass[SparkCopyCfg]
}
