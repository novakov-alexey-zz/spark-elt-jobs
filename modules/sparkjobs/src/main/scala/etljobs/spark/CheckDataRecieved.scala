package etljobs.spark

import etljobs.common.{ContextCfg, SparkOption}
import etljobs.spark.common.{sparkWithConfig, useResource}
import etljobs.common.MainArgsUtil._

import org.apache.spark.sql.functions.{lit, col}
import mainargs.{main, arg, ParserForMethods, ParserForClass}

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
    dateColumn: String
)

object CheckDataCfg {
  implicit def checkDataCfgParser = ParserForClass[CheckDataCfg]
}

case class EntityCount(entity: String, count: Long)

object CheckDataRecieved extends App {
  val DataRecivedCode = 0
  val DataAbsentCode = 99
  val EntityColumn = "entity"

  @main
  def run(cfg: CheckDataCfg) = {
    val sparkSession = sparkWithConfig(cfg.hadoopConfig).getOrCreate()
    val stats = useResource(sparkSession) { spark =>
      import spark.implicits._

      val chunks = cfg.entities.map { entity =>
        spark.read
          .format("delta")
          .load(s"${cfg.inputPath}/${cfg.ctx.dagId}/$entity")
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
      stats.map(r =>
        EntityCount(r.getAs[String](EntityColumn), r.getAs[Long]("count"))
      )
    println(s"current counts: ${counts.mkString(", ")}")
    val recieved = counts.forall(_.count > 0)

    val ec = if (recieved) DataRecivedCode else DataAbsentCode
    System.exit(ec)
  }

  ParserForMethods(this).runOrExit(args)
}
