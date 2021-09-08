package etljobs.emr

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig.{
  KEYGENERATOR_CLASS_NAME => _,
  _
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
    recordKey: Option[String]
)

object HudiCommon {
  val defaultHudiOptions = Map[String, String](
    OPERATION.key() -> UPSERT_OPERATION_OPT_VAL,
    TABLE_TYPE.key() -> "COPY_ON_WRITE",
    KEYGENERATOR_CLASS_NAME.key() -> "org.apache.hudi.keygen.CustomKeyGenerator",
    HIVE_PARTITION_EXTRACTOR_CLASS.key() -> classOf[
      MultiPartKeysValueExtractor
    ].getName,
    HIVE_STYLE_PARTITIONING.key() -> "true",
    INSERT_PARALLELISM_VALUE.key() -> "4",
    UPSERT_PARALLELISM_VALUE.key() -> "4",
    DELETE_PARALLELISM_VALUE.key() -> "4"
  )

  val conf = Map(
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.hive.convertMetastoreParquet" -> "false"
  )

  def getHudiWriterOptions(
      options: HudiWriterOptions
  ): Map[String, String] =
    Map[String, String](
      TBL_NAME.key() -> options.table,
      PARTITIONPATH_FIELD.key() -> options.partitionBy
        .map(_ + ":SIMPLE")
        .mkString(","),
      PRECOMBINE_FIELD.key() -> options.preCombineField.getOrElse(
        ""
      ),
      HIVE_SYNC_ENABLED.key() -> s"${options.syncToHive}",
      HIVE_DATABASE.key() -> options.syncDatabase.getOrElse("default"),
      HIVE_TABLE.key() -> options.table,
      HIVE_PARTITION_FIELDS.key() -> options.partitionBy.mkString(",")
    ) ++ options.recordKey.fold(Map.empty[String, String])(key =>
      Map(RECORDKEY_FIELD.key() -> key)
    ) ++ defaultHudiOptions

  def addColumns(df: DataFrame, executionDateCol: Column) =
    df
      .withColumn("execution_year", year(executionDateCol))
      .withColumn("execution_month", month(executionDateCol))
      .withColumn("execution_day", dayofmonth(executionDateCol))
}
