package etljobs.spark

import common._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming.StreamingQuery

import mainargs.{ParserForMethods, main}

object FileStreamToDataset extends App {

  private def waitForTermination(queries: List[StreamingQuery]) = {
    println(s"waiting for termination")
    queries.foreach(_.awaitTermination())
    println(s"all ${queries.length} streaming queries are terminated")
  }

  @main
  def run(cfg: SparkCopyCfg) = {
    val (input, output) = getInOutPaths(cfg.fileCopy)

    val sparkSession = SparkSession.builder.getOrCreate()
    useResource(sparkSession) { spark =>
      val queries = cfg.fileCopy.entityPatterns.map { entity =>
        val schema = getSchema(
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
          cfg.readerOptions.getOrElse(List.empty).foldLeft(stream) {
            case (acc, opt) => acc.option(opt.name, opt.value)
          }
        val df = streamWithOptions.load(input.toString())

        val outputPath = output.resolve(entity.name)
        val checkpointPath = outputPath.resolve("_checkpoints")
        println(
          s"starting stream for input '${input}' to output '${outputPath}'"
        )
        df.writeStream
          .outputMode(OutputMode.Append)
          .option("checkpointLocation", checkpointPath.toString())
          .trigger(Trigger.Once)
          .format(cfg.saveFormat.toSparkFormat)
          .start(outputPath.toString())
      }

      if (queries.nonEmpty)
        waitForTermination(queries)
    }

    if (requireMove(cfg))
      moveFiles(
        cfg.fileCopy.entityPatterns,
        cfg.fileCopy.processedDir,
        input
      )
  }

  ParserForMethods(this).runOrExit(args)
}
