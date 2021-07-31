package etljobs.emr

import etljobs.common.MainArgsUtil._
import etljobs.common.{ContextCfg, SparkOption}
import etljobs.emr.HudiCommon._
import etljobs.sparkcommon.DataFormat
import etljobs.sparkcommon.common.{
  dateLit,
  getSaveMode,
  sparkWithConfig,
  useResource
}
import mainargs._

import java.net.URI

@main
case class JoinTableSpec(
    table: String,
    recordKey: String,
    preCombineField: String
)

object JoinTableSpec {
  val errorMessage =
    "joinTableSpec must contain semicolon as separator of the table name, record key and preCombineField"
  implicit object EntityPatternRead
      extends TokensReader[JoinTableSpec](
        "joinTableSpec",
        strs => {
          assert(strs.nonEmpty, "joinTableSpec is empty")
          assert(
            strs.head.contains(":"),
            errorMessage
          )
          val entity = strs.head.split(":").toList
          entity match {
            case table :: key :: preCombineField :: _ =>
              Right(
                JoinTableSpec(table, key, preCombineField)
              )
            case _ =>
              Left(errorMessage)
          }
        }
      )
}

@main
case class JoinTableCfg(
    @arg(
      name = "hadoop-config",
      doc =
        "<name>:<value> list of options to be passed to hadoop configuration"
    )
    hadoopConfig: List[SparkOption],
    @arg(
      name = "table",
      doc = "table list to join."
    )
    tables: List[String],
    @arg(
      name = "sql-join",
      doc = "SQL query to perform join. Specified query will be executed as-is"
    )
    joinQuery: String,
    ctx: ContextCfg,
    @arg(
      name = "input-format",
      doc = "Data input format to be used by Spark Datasource API on read"
    )
    inputFormat: DataFormat,
    @arg(
      name = "output-format",
      doc = "Data output format to be used by Spark Datasource API on write"
    )
    saveFormat: DataFormat,
    @arg(short = 'i', doc = "Path to input directory")
    inputPath: URI,
    @arg(short = 'o', doc = "Output directory")
    outputPath: URI,
    @arg(
      name = "partition-by",
      doc = "Dataframe column list to partition by. Options: [year, month, day]"
    )
    partitionBy: List[String],
    @arg(
      name = "hudi-sync-to-hive",
      doc = "If set, Spark will sync Hudi table to Hive"
    )
    syncToHive: Flag,
    @arg(
      name = "sync-database",
      doc = "If set, Spark will sync Hudi table to Hive 'sync-database'"
    )
    syncDatabase: Option[String],
    @arg(
      name = "join-table",
      doc = "a table name to save the result of joined tables into"
    )
    joinTable: JoinTableSpec,
    @arg(
      doc = "Overwrite destination files/table if they exist"
    )
    overwrite: Flag
)

object JoinTableCfg {
  implicit def cfgParser: ParserForClass[JoinTableCfg] =
    ParserForClass[JoinTableCfg]
}

object TableJoin extends App {

  @main
  def run(
      cfg: JoinTableCfg
  ): Unit = {
    val session =
      sparkWithConfig(cfg.hadoopConfig, conf).getOrCreate()
    val executionDateCol = dateLit(
      cfg.ctx.executionDate
    )
    useResource(session) { spark =>
      cfg.tables.foreach { table =>
        val path = new URI(
          s"${cfg.inputPath}/${cfg.ctx.jobId}/$table"
        )
        println(s"table: $path")
        spark.read
          .format(cfg.inputFormat.toSparkFormat)
          .load(path.toString)
          .createOrReplaceTempView(table)
      }
      val writerOptions = HudiWriterOptions(
        cfg.partitionBy,
        cfg.syncToHive.value,
        cfg.syncDatabase,
        cfg.joinTable.table,
        Some(cfg.joinTable.preCombineField),
        Some(cfg.joinTable.recordKey)
      )
      val outputPath = new URI(s"${cfg.outputPath}/${cfg.joinTable.table}")
      println(s"output path: $outputPath")
      val query = cfg.joinQuery.replaceAll("\"", "")
      println(s"join query: $query")
      val saveMode = getSaveMode(cfg.overwrite.value)

      val df = addColumns(spark.sql(query), executionDateCol)
      df.write
        .options(getHudiWriterOptions(writerOptions))
        .partitionBy(cfg.partitionBy: _*)
        .mode(saveMode)
        .format(cfg.saveFormat.toSparkFormat)
        .save(outputPath.toString)
    }
  }

  ParserForMethods(this).runOrExit(args)
}
