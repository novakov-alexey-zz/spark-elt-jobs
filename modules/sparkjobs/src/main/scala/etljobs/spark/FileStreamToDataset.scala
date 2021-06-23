package etljobs.spark

import common._
import etljobs.common.FsUtil._
import etljobs.common.MainArgsUtil._
import etljobs.common.FileCopyCfg

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.streaming.StreamingQuery

import mainargs.{ParserForMethods, main, arg, ParserForClass}

import scala.io.Source
import java.nio.file.Path

@main
case class SparkStreamCopyCfg(
    copyCfg: SparkCopyCfg,
    @arg(
      short = 's',
      name = "schema-path",
      doc = "A path to schema directory for all entities as per entityPatterns"
    )
    schemaPath: Path
)

object SparkStreamCopyCfg {
  implicit def streamCfgParser = ParserForClass[SparkStreamCopyCfg]
}

object FileStreamToDataset extends App {
  private def getSchema(schemaPath: Path, entityName: String) = {
    val jsonSchema =
      Source
        .fromFile(schemaPath.resolve(s"$entityName.json").toFile())
        .getLines
        .mkString
    DataType.fromJson(jsonSchema).asInstanceOf[StructType]
  }

  private def getPaths(fileCopy: FileCopyCfg) = {
    val context =
      JobContext(
        fileCopy.dagId,
        fileCopy.executionDate
      )
    val output = contextDir(fileCopy.outputPath, context)
    val input = contextDir(fileCopy.inputPath, context)
    (input, output)
  }

  private def waitForTermination(queries: List[StreamingQuery]) = {
    println(s"waiting for termination")
    queries.foreach(_.awaitTermination())
    println(s"all ${queries.length} streaming queries are terminated")
  }

  @main
  def run(cfg: SparkStreamCopyCfg) = {
    val (input, output) = getPaths(cfg.copyCfg.fileCopy)

    val sparkSession = SparkSession.builder.getOrCreate()
    useResource(sparkSession) { spark =>
      val queries = cfg.copyCfg.fileCopy.entityPatterns.map { entity =>
        val schema = getSchema(cfg.schemaPath, entity.name)
        val stream = spark.readStream
          .option("pathGlobFilter", entity.globPattern)
          .format(cfg.copyCfg.inputFormat.toSparkFormat)
          .schema(schema)
        val streamWithOptions =
          cfg.copyCfg.readerOptions.getOrElse(List.empty).foldLeft(stream) {
            case (acc, opt) => acc.option(opt.name, opt.value)
          }
        val df = streamWithOptions.load(input.toString())

        val outputPath = output.resolve(entity.name)
        val checkpointPath = outputPath.resolve("checkpoint")
        println(
          s"starting stream for input '${input}' to output '${outputPath}'"
        )
        df.writeStream
          .outputMode(OutputMode.Append)
          .option("checkpointLocation", checkpointPath.toString())
          .trigger(Trigger.Once)
          .format(cfg.copyCfg.saveFormat.toSparkFormat)
          .start(outputPath.toString())
      }

      if (queries.nonEmpty)
        waitForTermination(queries)
    }
  }

  ParserForMethods(this).runOrExit(args)
}
