package etljobs.emr

import etljobs.common.HadoopCfg
import etljobs.emr.HudiCommon._
import etljobs.sparkcommon.SparkStreamingCopyCfg
import etljobs.sparkcommon.common._
import mainargs._
import org.apache.spark.sql.streaming.OutputMode

import java.net.URI

object HudiIngestor extends App {

  @main
  def run(
      cfg: SparkStreamingCopyCfg,
      @arg(
        doc = "run Job using Spark Structured streaming or regular Batch API"
      ) batchMode: Flag
  ): Unit = {
    val session =
      sparkWithConfig(cfg.sparkCopy.fileCopy.hadoopConfig, conf).getOrCreate()

    lazy val hadoopConf = HadoopCfg.get(cfg.sparkCopy.fileCopy.hadoopConfig)
    lazy val trigger = getTrigger(cfg.triggerInterval)
    lazy val executionDateCol = dateLit(
      cfg.sparkCopy.fileCopy.ctx.executionDate
    )

    useResource(session) { spark =>
      val queries = cfg.sparkCopy.fileCopy.entityPatterns.map { entity =>
        val (input, output) = getInOutPaths(cfg.sparkCopy.fileCopy)
        println(s"input path: $input")

        val schemaPath = cfg.sparkCopy.schemaPath.getOrElse(
          sys.error(
            s"struct schema is required for streaming query to load '${entity.name}' entity"
          )
        )
        val schema = readSchema(
          hadoopConf,
          schemaPath,
          entity.name
        )
        val outputPath = new URI(s"$output/${entity.name}")
        println(s"output path: $outputPath")

        val readerOptions =
          cfg.sparkCopy.readerOptions.map(o => o.name -> o.value).toMap
        val inputPattern = s"$input/${entity.globPattern}"
        println(s"inputPattern path: $inputPattern")

        val df =
          if (batchMode.value)
            spark.read
              .format(cfg.sparkCopy.inputFormat.toSparkFormat)
              .schema(schema)
              .options(readerOptions)
              .load(inputPattern)
          else
            spark.readStream
              .format(cfg.sparkCopy.inputFormat.toSparkFormat)
              .schema(schema)
              .options(readerOptions)
              .load(inputPattern)

        val dfWithColumns = addColumns(df, executionDateCol)
        val writerOptions = getHudiWriterOptions(
          HudiWriterOptions(
            cfg.sparkCopy.partitionBy,
            cfg.sparkCopy.syncToHive.value,
            cfg.sparkCopy.syncDatabase,
            entity.name,
            entity.preCombineField,
            entity.dedupKey
          )
        )
        println(s"writer options: $writerOptions")

        if (batchMode.value) {
          dfWithColumns.write
            .options(writerOptions)
            .partitionBy(cfg.sparkCopy.partitionBy: _*)
            .format(cfg.sparkCopy.saveFormat.toSparkFormat)
            .save(outputPath.toString)
          () => ()
        } else {
          val checkpointPath = new URI(s"$outputPath/_checkpoints")
          val query = dfWithColumns.writeStream
            .options(writerOptions)
            .option("checkpointLocation", checkpointPath.toString)
            .partitionBy(cfg.sparkCopy.partitionBy: _*)
            .format(cfg.sparkCopy.saveFormat.toSparkFormat)
            .outputMode(OutputMode.Append)
            .trigger(trigger)
            .start(outputPath.toString)
          () => {
            println(
              s"waiting for query termination"
            )
            query.awaitTermination()
          }
        }
      }

      queries.foreach(cb => cb())
    }
  }

  ParserForMethods(this).runOrExit(args)
}
