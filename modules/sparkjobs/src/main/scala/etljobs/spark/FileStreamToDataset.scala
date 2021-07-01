package etljobs.spark

import common._
import etljobs.common.HadoopCfg

import org.apache.spark.sql.streaming.{OutputMode, Trigger, StreamingQuery}
import mainargs.{ParserForMethods, main}
import java.net.URI

object FileStreamToDataset extends App {

  private def waitForTermination(queries: List[StreamingQuery]) = {
    println(s"waiting for termination")
    queries.foreach(_.awaitTermination())
    println(s"all ${queries.length} streaming queries are terminated")
  }

  @main
  def run(cfg: SparkCopyCfg) = {
    val (input, output) = getInOutPaths(cfg.fileCopy)
    val sparkSession = sparkWithConfig(cfg.fileCopy.hadoopConfig).getOrCreate()
    lazy val conf = HadoopCfg.get(cfg.fileCopy.hadoopConfig)

    useResource(sparkSession) { spark =>
      val queries = cfg.fileCopy.entityPatterns.map { entity =>
        val schema = readSchema(
          conf,
          cfg.schemaPath.getOrElse(
            sys.error(
              s"struct schema is required for streaming query to load '${entity.name}'' entity"
            )
          ),
          entity.name
        )
        val stream = spark.readStream
          .option("pathGlobFilter", entity.globPattern)
          .format(cfg.inputFormat.toSparkFormat)
          .schema(schema)
        val streamWithOptions =
          cfg.readerOptions.foldLeft(stream) {
            case (acc, opt) => acc.option(opt.name, opt.value)
          }
        val df = streamWithOptions
          .load(input.toString())
          .withColumn(
            "date",
            dateLit(cfg.fileCopy.ctx.executionDate)
          )

        val outputPath = new URI(s"$output/${entity.name}")
        val checkpointPath = new URI(s"$outputPath/_checkpoints")
        println(
          s"starting stream for input '${input}' to output '${outputPath}'"
        )
        df.writeStream
          .outputMode(OutputMode.Append)
          .option("checkpointLocation", checkpointPath.toString())
          .partitionBy(cfg.partitionBy)
          .trigger(Trigger.Once)
          .format(cfg.saveFormat.toSparkFormat)
          .start(outputPath.toString())
      }

      if (queries.nonEmpty)
        waitForTermination(queries)
    }

    if (requireMove(cfg)) {
      moveFiles(
        conf,
        cfg.fileCopy.entityPatterns,
        cfg.fileCopy.processedDir,
        input
      )
    }
  }

  ParserForMethods(this).runOrExit(args)
}
