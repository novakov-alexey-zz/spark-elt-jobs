package etljobs.spark

import etljobs.common.{ContextCfg, SparkOption}
import etljobs.sparkcommon.common.{sparkWithConfig, useResource}
import etljobs.sparkcommon._
import etljobs.common.MainArgsUtil._
import mainargs.{ParserForClass, ParserForMethods, arg, main}
import org.apache.spark.sql.functions.{col, lit}

import java.net.URI

@main
case class CheckDataCfg(
    ctx: ContextCfg,
    @arg(
      doc = "list of entity paths to check data is received for"
    )
    entities: List[String],
    @arg(
      name = "hadoop-config",
      doc =
        "<name>:<value> list of options to be passed to hadoop configuration"
    )
    hadoopConfig: List[SparkOption],
    @arg(short = 'i', doc = "Path to input directory")
    inputPath: URI,
    @arg(name = "date-column", doc = "Date column name")
    dateColumn: String,
    @arg(
      name = "input-format",
      doc = "Data input format to be used by Spark Datasource API on read"
    )
    inputFormat: DataFormat
)

object CheckDataCfg {
  implicit def checkDataCfgParser: ParserForClass[CheckDataCfg] =
    ParserForClass[CheckDataCfg]
}

object CheckDataReceived extends App {
  val DataRecivedCode = 0
  val DataAbsentCode = 99
  val EntityColumn = "entity"

  @main
  def run(cfg: CheckDataCfg): Unit = {
    val sparkSession = sparkWithConfig(cfg.hadoopConfig).getOrCreate()
    val stats = useResource(sparkSession) { spark =>
      import spark.implicits._

      val chunks = cfg.entities.map { entity =>
        val tablePath = s"${cfg.inputPath}/${cfg.ctx.jobId}/$entity"
        println(s"table path: $tablePath")
        spark.read
          .format(cfg.inputFormat.toSparkFormat)
          .load(tablePath)
          .select(col(cfg.dateColumn))
          .filter(col(cfg.dateColumn) === lit(cfg.ctx.executionDate))
          .withColumn(EntityColumn, lit(entity))
      }
      val emptyDF =
        Seq.empty[(String, String)].toDF(Seq(cfg.dateColumn, EntityColumn): _*)
      val union = chunks.foldLeft(emptyDF)(_.union(_))
      union.groupBy(EntityColumn).count().collect()
    }
    val counts =
      stats
        .map(r => r.getAs[String](EntityColumn) -> r.getAs[Long]("count"))
        .toMap
    val received = cfg.entities.forall(e => counts.getOrElse(e, 0L) > 0)
    println(
      s"All data received: $received, current counts: ${if (counts.isEmpty) "none"
      else counts}"
    )

    val ec = if (received) DataRecivedCode else DataAbsentCode
    System.exit(ec)
  }

  ParserForMethods(this).runOrExit(args)
}
