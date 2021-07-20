package etljobs.glue


import org.apache.spark.sql.{SaveMode, SparkSession}

import java.net.URI
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hive.MultiPartKeysValueExtractor

object HudiDemo {

  def run(session: SparkSession, outputPath: URI): Unit = {
    import session.implicits._

    val inputDF = Seq(
      ("100", "2015-01-01", "2015-01-01T13:51:39.340396Z"),
      ("101", "2015-01-01", "2015-01-01T12:14:58.597216Z"),
      ("102", "2015-01-01", "2015-01-01T13:51:40.417052Z"),
      ("103", "2015-01-01", "2015-01-01T13:51:40.519832Z"),
      ("104", "2015-01-02", "2015-01-01T12:15:00.512679Z"),
      ("105", "2015-01-02", "2015-01-01T13:51:42.248818Z")
    ).toDF("id", "creation_date", "last_update_time")

    val hudiOptions = Map[String, String](
      HoodieWriteConfig.TABLE_NAME -> "my_hudi_table",
      TABLE_TYPE_OPT_KEY -> "COPY_ON_WRITE",
      RECORDKEY_FIELD_OPT_KEY -> "id",
      PARTITIONPATH_FIELD_OPT_KEY -> "creation_date",
      PRECOMBINE_FIELD_OPT_KEY -> "last_update_time",
      HIVE_SYNC_ENABLED_OPT_KEY -> "true",
      HIVE_TABLE_OPT_KEY -> "my_hudi_table",
      HIVE_PARTITION_FIELDS_OPT_KEY -> "creation_date",
      HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY -> classOf[MultiPartKeysValueExtractor].getName
    )

    inputDF.write
      .format("hudi")
      .option(OPERATION_OPT_KEY, INSERT_OPERATION_OPT_VAL)
      .options(hudiOptions)
      .mode(SaveMode.Overwrite)
      .save(outputPath.toString)
  }
}
