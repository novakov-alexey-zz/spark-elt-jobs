// scala 2.13

import $ivy.`com.github.seratch::awscala-s3:0.9.2`
import $ivy.`org.scalacheck::scalacheck:1.14.3`

import org.scalacheck.Gen
import awscala._
import s3._
import com.amazonaws.services.s3.model.ObjectMetadata
import java.util.Date
import Order._

implicit val s3Api = S3.at(Region.EU_CENTRAL_1)

implicit class CSVWrapper(val prod: Product) extends AnyVal {
  def toCSV() = prod.productIterator
    .map {
      case Some(value) => value
      case None        => ""
      case rest        => rest
    }
    .mkString(",")
}

case class Order(
    orderId: Int,
    customerId: Int,
    itemId: Int,
    quantity: Int,
    year: Int,
    month: Int,
    day: Int,
    lastUpdateTime: Long
)

object Order {
  val orderMax = 100000
  val orderIdGen = Gen.choose(1, orderMax)
  val customerIdGen = Gen.choose(1, 100)
  val itemIdGen = Gen.choose(1, 1000)
  val quantityGen = Gen.choose(2, 20)
  val yearGen = Gen.const(2021)
  val monthGen = Gen.choose(7, 7)
  val dayMin = 20
  val dayMax = 25
  val dayGen = Gen.choose(dayMin, dayMax)
  val batchSizeGen = Gen.choose(100, 400)

  val csvHeader =
    "orderId,customerId,itemId,quantity,year,month,day,last_update_time"
}

def genOrder =
  for {
    orderId <- orderIdGen
    customerId <- customerIdGen
    itemId <- itemIdGen
    quantity <- quantityGen
    year <- yearGen
    month <- monthGen
    day <- dayGen
  } yield Order(
    orderId,
    customerId,
    itemId,
    quantity,
    year,
    month,
    day,
    System.currentTimeMillis
  )

def genRecords =
  for {
    size <- batchSizeGen
    events <- Gen.listOfN(size, genOrder)
  } yield events

def putFile(inBucket: Bucket, targetBucketFolder: String) = {
  val records = genRecords.sample.getOrElse(Nil)
  val text = Order.csvHeader + "\n" + records.map(_.toCSV).mkString("\n")
  val payload = text.getBytes("UTF-8")
  val uri = s"$targetBucketFolder/orders_${System.currentTimeMillis}.csv"
  val meta = {
    val om = new ObjectMetadata
    om.setContentLength(payload.length)
    om
  }
  inBucket.putObject(uri, payload, meta)
  records
}

@main
def main(
    targetBucket: String @arg(
      doc = "target bucket name to put data into"
    ),
    @arg(
      doc = "target bucket folder to put data into"
    )
    targetBucketFolder: String,
    @arg(
      doc = "delay between uploading a CSV file to S3 bucket"
    )
    delay: Long = 1000
) = {
  val inBucket = s3Api
    .bucket(targetBucket)
    .getOrElse(
      sys.error(
        s"Bucket '$targetBucket' is not found in ${Region.EU_CENTRAL_1} region"
      )
    )

  var uniqueOrders = Set.empty[String]
  var count = 0
  val expectedCount = (dayMax - dayMin + 1) * orderMax

  Iterator
    .continually(putFile(inBucket, targetBucketFolder))
    .takeWhile(_ => uniqueOrders.size < expectedCount)
    .foreach { recs =>
      count += recs.length
      uniqueOrders ++= recs.map(r => r.orderId + "_" + r.day).toSet
      print(
        s"[${new Date()}] sent unique current / unique expected / total records: ${uniqueOrders.size} / $expectedCount / $count\r"
      )
      Thread.sleep(delay)
    }
}
