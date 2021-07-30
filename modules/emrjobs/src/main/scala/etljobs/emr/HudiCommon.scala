package etljobs.emr

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig.{
  DELETE_PARALLELISM,
  INSERT_PARALLELISM,
  TABLE_NAME,
  UPSERT_PARALLELISM
}
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.spark.sql.functions.{dayofmonth, month, year}
import org.apache.spark.sql.{Column, DataFrame}

case class HudiWriterOptions(
    partitionBy: List[String],
    syncToHive: Boolean,
    syncDatabase: Option[String],
    table: String,
    preCombineField: Option[String],
    dedupKey: Option[String]
)

object HudiCommon {
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

  def getHudiWriterOptions(
      options: HudiWriterOptions
  ): Map[String, String] =
    Map[String, String](
      TABLE_NAME -> options.table,
      PARTITIONPATH_FIELD_OPT_KEY -> options.partitionBy
        .map(_ + ":SIMPLE")
        .mkString(","),
      PRECOMBINE_FIELD_OPT_KEY -> options.preCombineField.getOrElse(
        ""
      ),
      HIVE_SYNC_ENABLED_OPT_KEY -> s"${options.syncToHive}",
      HIVE_DATABASE_OPT_KEY -> options.syncDatabase.getOrElse("default"),
      HIVE_TABLE_OPT_KEY -> options.table,
      HIVE_PARTITION_FIELDS_OPT_KEY -> options.partitionBy.mkString(",")
    ) ++ options.dedupKey.fold(Map.empty[String, String])(key =>
      Map(RECORDKEY_FIELD_OPT_KEY -> key)
    ) ++ defaultHudiOptions

  def addColumns(df: DataFrame, executionDateCol: Column) =
    df
      .withColumn("execution_year", year(executionDateCol))
      .withColumn("execution_month", month(executionDateCol))
      .withColumn("execution_day", dayofmonth(executionDateCol))
}
