package etljobs.sparkcommon

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.types.StructType


object udaf {
  val spark: SparkSession = SparkSession.builder().getOrCreate()

  case class FirstNonEmpty(schema: StructType)
      extends Aggregator[Row, Row, Row]
      with Serializable {

    def zero = new GenericRowWithSchema(Array.empty, schema)
    def reduce(acc: Row, x: Row) = {
      val vals = x.schema.fields.indices.map(i =>
        if (
          acc.length > i &&
          !acc.isNullAt(i)
        ) acc.get(i)
        else x.get(i)
      )
      new GenericRowWithSchema(vals.toArray, schema)
    }

    def merge(acc1: Row, acc2: Row) = reduce(acc1, acc2)
    def finish(acc: Row) = acc

    def bufferEncoder: Encoder[Row] = RowEncoder(
      schema
    )
    def outputEncoder: Encoder[Row] = RowEncoder(
      schema
    )
  }

  val data = Seq(
    (1, "NA", None, Some(100.0), None),
    (1, "MX", Some(100), None, None),
    (1, "MX", Some(50), Some(200.0), None),
    (1, "MX", None, Some(200.0), Some("Active")),
    (1, "NA", Some(200), None, Some("Inactive")),
    (2, "NA", Some(300), None, Some("Active"))
  )

  val df = spark
    .createDataFrame(data)
    .toDF("regionId", "regionName", "units", "amount", "status")
  df.createOrReplaceTempView("transactions")
  spark.udf.register(
    "firstNonEmpty",
    functions.udaf(FirstNonEmpty(df.schema), RowEncoder(df.schema))
  )
  spark
    .sql(
      """
        | select collapsed.* from (
        |   select regionId, regionName, firstNonEmpty(*) as collapsed from (select * from transactions order by regionId, regionName, units)
        |   group by regionId, regionName
        | )""".stripMargin
    )
    .show(false)


  val dates = Seq(
    (1, 1, 2000, 946684800L)
  )
  val df2 = spark
    .createDataFrame(dates)
    .toDF("zdmonthday", "zdmonth", "zdyear", "zdstartofday")

  df2.createOrReplaceTempView("transactions")
}
