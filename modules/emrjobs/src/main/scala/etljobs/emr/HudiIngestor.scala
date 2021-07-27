package etljobs.emr

import etljobs.common.{EntityPattern, HadoopCfg}
import etljobs.sparkcommon.common._
import etljobs.sparkcommon.{SparkCopyCfg, SparkStreamingCopyCfg}
import mainargs._
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
import org.apache.spark.sql.{Column, DataFrame}

import java.net.URI

object HudiIngestor extends App {
  val defaultHudiOptions = Map[String, String](
    OPERATION_OPT_KEY -> UPSERT_OPERATION_OPT_VAL,
    TABLE_TYPE_OPT_KEY -> "COPY_ON_WRITE",
    KEYGENERATOR_CLASS_OPT_KEY -> "org.apache.hudi.keygen.CustomKeyGenerator",
    HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY -> classOf[
      MultiPartKeysValueExtractor
    ].getName,
    HIVE_STYLE_PARTITIONING_OPT_KEY -> "true",
    INSERT_PARALLELISM -> "4",
    UPSERT_PARALLELISM -> "4",
    DELETE_PARALLELISM -> "4"
  )

  val conf = Map(
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.hive.convertMetastoreParquet" -> "false"
  )

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
        val writerOptions = hudiWriterOptions(cfg.sparkCopy, entity)
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

  private def addColumns(df: DataFrame, executionDateCol: Column) =
    df
      .withColumn("execution_year", year(executionDateCol))
      .withColumn("execution_month", month(executionDateCol))
      .withColumn("execution_day", dayofmonth(executionDateCol))

  private def hudiWriterOptions(
      sparkCopy: SparkCopyCfg,
      entity: EntityPattern
  ) =
    Map[String, String](
      TABLE_NAME -> entity.name,
      PARTITIONPATH_FIELD_OPT_KEY -> sparkCopy.partitionBy
        .map(_ + ":SIMPLE")
        .mkString(","),
      PRECOMBINE_FIELD_OPT_KEY -> entity.preCombineField.getOrElse(
        ""
      ),
      HIVE_SYNC_ENABLED_OPT_KEY -> s"${sparkCopy.syncToHive.value}",
      HIVE_DATABASE_OPT_KEY -> sparkCopy.syncDatabase.getOrElse("default"),
      HIVE_TABLE_OPT_KEY -> entity.name,
      HIVE_PARTITION_FIELDS_OPT_KEY -> sparkCopy.partitionBy.mkString(",")
    ) ++ entity.dedupKey.fold(Map.empty[String, String])(key =>
      Map(RECORDKEY_FIELD_OPT_KEY -> key)
    ) ++ defaultHudiOptions

  ParserForMethods(this).runOrExit(args)
}
