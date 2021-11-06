package etljobs.glue

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.hudi.keygen.constant.KeyGeneratorOptions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.net.URI

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
      TBL_NAME.key() -> "my_hudi_table",
      TABLE_TYPE.key() -> "COPY_ON_WRITE",
      RECORDKEY_FIELD_NAME.key() -> "id",
      PARTITIONPATH_FIELD_NAME.key() -> "creation_date",
      PRECOMBINE_FIELD_NAME.key() -> "last_update_time",
      HIVE_SYNC_ENABLED.key() -> "true",
      HIVE_TABLE.key() -> "my_hudi_table",
      HIVE_PARTITION_FIELDS.key() -> "creation_date",
      HIVE_PARTITION_EXTRACTOR_CLASS.key() -> classOf[
        MultiPartKeysValueExtractor
      ].getName
    )

    inputDF.write
      .format("hudi")
      .option(OPERATION.key(), INSERT_OPERATION_OPT_VAL)
      .options(hudiOptions)
      .mode(SaveMode.Overwrite)
      .save(outputPath.toString)
  }
}
