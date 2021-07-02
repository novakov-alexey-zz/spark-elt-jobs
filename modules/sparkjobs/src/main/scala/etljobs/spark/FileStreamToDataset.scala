package etljobs.spark

import common._
import etljobs.common.HadoopCfg
import etljobs.common.SparkOption

import org.apache.spark.sql.streaming.{OutputMode, Trigger, StreamingQuery}
import org.apache.spark.sql.streaming.DataStreamReader
import mainargs.{ParserForMethods, main}

import java.net.URI

object FileStreamToDataset extends App {

  private def waitForTermination(queries: List[StreamingQuery]) = {
    println(s"waiting for termination")
    queries.foreach(_.awaitTermination())
    println(s"all ${queries.length} streaming queries are terminated")
  }

  private def addOptions(
      stream: DataStreamReader,
      cfg: SparkCopyCfg,
      globPattern: String
  ) = {
    val archivingOptions = if (cfg.archiveSource.value) {
      val archiveDir = getInPath(
        cfg.fileCopy.copy(inputPath =
          new URI(s"${cfg.fileCopy.inputPath}/archive")
        )
      ).toString()
      List(
        SparkOption("cleanSource", "archive"),
        SparkOption(
          "sourceArchiveDir",
          archiveDir
        )
      )
    } else Nil

    val autoOptions =
      List(SparkOption("pathGlobFilter", globPattern)) ++ archivingOptions

    (cfg.readerOptions ++ autoOptions).foldLeft(stream) { case (acc, opt) =>
      acc.option(opt.name, opt.value)
    }
  }

  @main
  def run(cfg: SparkCopyCfg) = {
    val (input, output) = getInOutPaths(cfg.fileCopy)
    val sparkSession = sparkWithConfig(cfg.fileCopy.hadoopConfig).getOrCreate()
    lazy val conf = HadoopCfg.get(cfg.fileCopy.hadoopConfig)

    useResource(sparkSession) { spark =>
      val queries = cfg.fileCopy.entityPatterns.map { entity =>
        val schemaPath = cfg.schemaPath.getOrElse(
          sys.error(
            s"struct schema is required for streaming query to load '${entity.name}'' entity"
          )
        )
        val schema = readSchema(
          conf,
          schemaPath,
          entity.name
        )
        val stream = addOptions(
          spark.readStream,
          cfg,
          entity.globPattern
        ).format(cfg.inputFormat.toSparkFormat)
          .schema(schema)
        val df = stream
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

    if (requireMove(cfg) && !cfg.archiveSource.value) {
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
