package etljobs.emr

import etljobs.common.HadoopCfg
import etljobs.sparkcommon.SparkStreamingCopyCfg
import etljobs.sparkcommon.common._
import mainargs.{ParserForMethods, main}
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig.{
  DELETE_PARALLELISM,
  INSERT_PARALLELISM,
  TABLE_NAME,
  UPSERT_PARALLELISM
}
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.spark.sql.functions.{dayofmonth, month, year}
import org.apache.spark.sql.streaming.OutputMode

import java.net.URI

object HudiIngestor extends App {

  @main
  def run(cfg: SparkStreamingCopyCfg): Unit = {
    val conf = Map(
      "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
      "spark.sql.hive.convertMetastoreParquet" -> "false"
    )
    val session =
      sparkWithConfig(cfg.sparkCopy.fileCopy.hadoopConfig, conf).getOrCreate()

    lazy val hadoopConf = HadoopCfg.get(cfg.sparkCopy.fileCopy.hadoopConfig)
    lazy val trigger = getTrigger(cfg.triggerInterval)

    useResource(session) { spark =>
      val queries = cfg.sparkCopy.fileCopy.entityPatterns.map { entity =>
        val (input, output) = getInOutPaths(cfg.sparkCopy.fileCopy)
        println(s"input path: $input")
        val executionDateCol = dateLit(cfg.sparkCopy.fileCopy.ctx.executionDate)
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
        val stream =
          spark.readStream
            .format(cfg.sparkCopy.inputFormat.toSparkFormat)
            .schema(schema)
        val streamWithOptions = cfg.sparkCopy.readerOptions.foldLeft(stream) {
          case (acc, opt) =>
            acc.option(opt.name, opt.value)
        }
        val df = streamWithOptions
          .load(s"$input/*.csv")
          .withColumn("execution_year", year(executionDateCol))
          .withColumn("execution_month", month(executionDateCol))
          .withColumn("execution_day", dayofmonth(executionDateCol))

        val outputPath = new URI(s"$output/${entity.name}")
        println(s"output path: $outputPath")

        val checkpointPath = new URI(s"$outputPath/_checkpoints")
        val partitionFields = cfg.sparkCopy.partitionBy.mkString(",")
        val hudiWriterOptions = Map[String, String](
          TABLE_NAME -> entity.name,
          TABLE_TYPE_OPT_KEY -> "COPY_ON_WRITE",
          KEYGENERATOR_CLASS_OPT_KEY -> "org.apache.hudi.keygen.CustomKeyGenerator",
          PARTITIONPATH_FIELD_OPT_KEY -> cfg.sparkCopy.partitionBy
            .map(_ + ":SIMPLE")
            .mkString(","),
          PRECOMBINE_FIELD_OPT_KEY -> "last_update_time",
          HIVE_SYNC_ENABLED_OPT_KEY -> s"${cfg.sparkCopy.syncToHive.value}",
          HIVE_TABLE_OPT_KEY -> entity.name,
          HIVE_PARTITION_FIELDS_OPT_KEY -> partitionFields,
          HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY -> classOf[
            MultiPartKeysValueExtractor
          ].getName,
          HIVE_STYLE_PARTITIONING_OPT_KEY -> "true",
          INSERT_PARALLELISM -> "4",
          UPSERT_PARALLELISM -> "4",
          DELETE_PARALLELISM -> "4"
        ) ++ entity.dedupKey.fold(Map.empty[String, String])(key =>
          Map(RECORDKEY_FIELD_OPT_KEY -> key)
        )
        df.writeStream
          .option(OPERATION_OPT_KEY, UPSERT_OPERATION_OPT_VAL)
          .options(hudiWriterOptions)
          .outputMode(OutputMode.Append)
          .option("checkpointLocation", checkpointPath.toString)
          .partitionBy(cfg.sparkCopy.partitionBy: _*)
          .trigger(trigger)
          .format(cfg.sparkCopy.saveFormat.toSparkFormat)
          .start(outputPath.toString)
      }
      println(s"waiting for termination")
      queries.foreach(_.awaitTermination())
      println(s"all ${queries.length} streaming queries are terminated")
    }
  }

  ParserForMethods(this).runOrExit(args)
}
