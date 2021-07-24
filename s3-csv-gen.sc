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
  val orderIdGen = Gen.choose(1, 100)
  val customerIdGen = Gen.choose(1, 100)
  val itemIdGen = Gen.choose(1, 1000)
  val quantityGen = Gen.choose(2, 20)
  val yearGen = Gen.const(2021)
  val monthGen = Gen.choose(1, 12)
  val dayGen = Gen.choose(1, 30)
  val batchSizeGen = Gen.choose(10, 100)

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

def genCsvRecords: Gen[String] =
  for {
    size <- batchSizeGen
    events <- Gen.listOfN(size, genOrder)
    dataBatch = events.map(_.toCSV).mkString("\n")
  } yield dataBatch

def putFile(inBucket: Bucket, targetBucketFolder: String) = {
  val records = genCsvRecords.sample.getOrElse("")
  println(records)
  val text = Order.csvHeader + "\n" + records
  val payload = text.getBytes("UTF-8")
  val uri = s"$targetBucketFolder/${System.currentTimeMillis}"
  inBucket.putObject(uri, payload, new ObjectMetadata)
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

  Iterator
    .continually(putFile(inBucket, targetBucketFolder))
    .takeWhile(_ => true)
    .foreach { _ =>
      val currentSeconds = System.currentTimeMillis() / 1000
      if (currentSeconds % 60 == 0)
        println(s"New window at: ${new Date()}")
      Thread.sleep(delay)
    }
}
