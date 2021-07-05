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
    readerOptions: List[SparkOption],
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
    fileCopy: FileCopyCfg,
    @arg(
      name = "stream-move-files",
      doc = "Whether to move source files using Spark streaming 'cleanSource' feature"
    )
    streamMoveFiles: Flag
)

object SparkCopyCfg {
  implicit def copyParamsParser = ParserForClass[SparkCopyCfg]
}

@main
case class SparkStreamingCopyCfg(
    sparkCopy: SparkCopyCfg,
    @arg(
      name = "trigger-interval",
      doc =
        "Number of milliseconds for Spark streaming ProcessingTime trigger. Negative value sets Trigger.Once"
    )
    triggerInterval: Long
)

object SparkStreamingCopyCfg {
  implicit def copyParamsParser = ParserForClass[SparkStreamingCopyCfg]
}
