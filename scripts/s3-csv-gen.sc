// scala 2.13

import $ivy.`com.github.seratch::awscala-s3:0.9.2`
import $ivy.`org.scalacheck::scalacheck:1.14.3`

import org.scalacheck.Gen
import awscala._
import s3._
import com.amazonaws.services.s3.model.ObjectMetadata
import java.util.Date
import Order._
import Item._
import GenCfg._
import GenParams._
import mainargs._

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

object GenParams {
  val itemIdGen = Gen.choose(1, 1000)
  val yearGen = Gen.const(2021)
  val monthGen = Gen.choose(7, 7)
  val dayMin = 20
  val dayMax = 25
  val dayGen = Gen.choose(dayMin, dayMax)
  val batchSizeGen = Gen.choose(100, 400)
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

  val quantityGen = Gen.choose(2, 20)

  val csvHeader =
    "orderId,customerId,itemId,quantity,year,month,day,last_update_time"

  def gen =
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
}

def genRecords[T](gen: Gen[T]): Gen[List[T]] =
  for {
    size <- batchSizeGen
    events <- Gen.listOfN(size, gen)
  } yield events

case class Item(
    itemId: Int,
    name: String,
    price: Double,
    year: Int,
    month: Int,
    day: Int,
    lastUpdateTime: Long
)

object Item {
  // strGen generates a fixed length random string
  val strGen = (n: Int) => Gen.listOfN(n, Gen.alphaChar).map(_.mkString)

  val itemMax = 10000
  val nameGen = strGen(10)
  val priceGen = Gen.choose(0.1, 50)

  val csvHeader = "itemId,name,price,year,month,day,last_update_time"

  def gen = for {
    itemId <- itemIdGen
    name <- nameGen
    price <- priceGen
    year <- yearGen
    month <- monthGen
    day <- dayGen
  } yield Item(
    itemId,
    name,
    price,
    year,
    month,
    day,
    System.currentTimeMillis
  )
}

case class PutFileCfg[T](
    filePrefix: String,
    csvHeader: String,
    gen: Gen[List[T]],
    inBucket: Bucket,
    targetBucketFolder: String
)

def putFile[T <: Product](putCfg: PutFileCfg[T]): List[T] = {
  val records = putCfg.gen.sample.getOrElse(List.empty[T])
  val text = putCfg.csvHeader + "\n" + records.map(_.toCSV).mkString("\n")
  val payload = text.getBytes("UTF-8")
  val uri =
    s"${putCfg.targetBucketFolder}/${putCfg.filePrefix}_${System.currentTimeMillis}.csv"
  val meta = {
    val om = new ObjectMetadata
    om.setContentLength(payload.length)
    om
  }
  putCfg.inBucket.putObject(uri, payload, meta)
  records
}

@main
case class GenCfg(
    targetBucket: String @arg(
      doc = "target bucket name to put data into"
    ),
    @arg(
      doc = "target bucket folder to put data into"
    )
    targetBucketFolder: String
)

object GenCfg {
  implicit def genCfgParser: ParserForClass[GenCfg] = ParserForClass[GenCfg]
}

def getInputBucket(targetBucket: String, region: Region = Region.EU_CENTRAL_1) =
  s3Api
    .bucket(targetBucket)
    .getOrElse(
      sys.error(
        s"Bucket '$targetBucket' is not found in $region region"
      )
    )

@main
def generateItems(
    cfg: GenCfg,
    @arg(
      doc = "delay between uploading a CSV file to S3 bucket"
    )
    delay: Long = 1000
) = {
  val inBucket = getInputBucket(cfg.targetBucket)
  var uniqueItems = Set.empty[String]
  var count = 0
  val expectedCount = (dayMax - dayMin + 1) * itemMax
  val putCfg = PutFileCfg[Item](
    "items",
    Item.csvHeader,
    genRecords(Item.gen),
    inBucket,
    cfg.targetBucketFolder
  )

  Iterator
    .continually(putFile(putCfg))
    .takeWhile(_ => uniqueItems.size < expectedCount)
    .foreach { recs =>
      count += recs.length
      uniqueItems ++= recs.map(r => r.itemId + "_" + r.day).toSet
      print(
        s"[${new Date()}] sent unique current / unique expected / total records: ${uniqueItems.size} / $expectedCount / $count\r"
      )
      Thread.sleep(delay)
    }
}

@main
def generateOrders(
    cfg: GenCfg,
    @arg(
      doc = "delay between uploading a CSV file to S3 bucket"
    )
    delay: Long = 1000
) = {
  val inBucket = getInputBucket(cfg.targetBucket)
  var uniqueOrders = Set.empty[String]
  var count = 0
  val expectedCount = (dayMax - dayMin + 1) * orderMax
  val putCfg = PutFileCfg[Order](
    "orders",
    Order.csvHeader,
    genRecords(Order.gen),
    inBucket,
    cfg.targetBucketFolder
  )

  Iterator
    .continually(putFile(putCfg))
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
