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

  private def archiveSourceOptions(cfg: SparkCopyCfg) =
    if (cfg.archiveSource.value) {
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
        // For DEBUG: below are two Spark INTERNAL options to speed up the source archiving process
        ,
        SparkOption("spark.sql.streaming.fileSource.log.compactInterval", "0"),
        SparkOption("spark.sql.streaming.fileSource.log.cleanupDelay", "1")
      )
    } else Nil

  private def addOptions(
      stream: DataStreamReader,
      cfg: SparkCopyCfg,
      globPattern: String
  ) = {
    val archivingOptions = archiveSourceOptions(cfg)
    val autoOptions =
      List(SparkOption("pathGlobFilter", globPattern)) ++ archivingOptions

    (cfg.readerOptions ++ autoOptions).foldLeft(stream) { case (acc, opt) =>
      acc.option(opt.name, opt.value)
    }
  }

  @main
  def run(cfg: SparkStreamingCopyCfg) = {
    val sparkCopy = cfg.sparkCopy
    val (input, output) = getInOutPaths(sparkCopy.fileCopy)
    val sparkSession =
      sparkWithConfig(sparkCopy.fileCopy.hadoopConfig).getOrCreate()
    lazy val conf = HadoopCfg.get(cfg.sparkCopy.fileCopy.hadoopConfig)

    useResource(sparkSession) { spark =>
      val queries = sparkCopy.fileCopy.entityPatterns.map { entity =>
        val schemaPath = sparkCopy.schemaPath.getOrElse(
          sys.error(
            s"struct schema is required for streaming query to load '${entity.name}' entity"
          )
        )
        val schema = readSchema(
          conf,
          schemaPath,
          entity.name
        )
        val stream = addOptions(
          spark.readStream,
          sparkCopy,
          entity.globPattern
        ).format(sparkCopy.inputFormat.toSparkFormat)
          .schema(schema)
        val df = stream
          .load(input.toString())
          .withColumn(
            "date",
            dateLit(sparkCopy.fileCopy.ctx.executionDate)
          )

        val outputPath = new URI(s"$output/${entity.name}")
        val checkpointPath = new URI(s"$outputPath/_checkpoints")
        val trigger =
          if (cfg.triggerInterval < 0) Trigger.Once
          else
            Trigger.ProcessingTime(cfg.triggerInterval)
        println(
          s"starting stream for input '${input}' to output '${outputPath}' with trigger $trigger"
        )
        df.writeStream
          .outputMode(OutputMode.Append)
          .option("checkpointLocation", checkpointPath.toString())
          .partitionBy(sparkCopy.partitionBy)
          .trigger(trigger)
          .format(sparkCopy.saveFormat.toSparkFormat)
          .start(outputPath.toString())
      }

      if (queries.nonEmpty)
        waitForTermination(queries)
    }

    if (requireMove(sparkCopy) && !sparkCopy.archiveSource.value) {
      moveFiles(
        conf,
        sparkCopy.fileCopy.entityPatterns,
        sparkCopy.fileCopy.processedDir,
        input
      )
    }
  }

  ParserForMethods(this).runOrExit(args)
}
