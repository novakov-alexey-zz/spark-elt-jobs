//> using scala "3.1"

import $ivy.`com.github.seratch::awscala-s3:0.9.2`
import $ivy.`org.scalacheck::scalacheck:1.16.0`

import org.scalacheck.Gen
import awscala.*
import com.amazonaws.services.s3.model.ObjectMetadata
import s3.*

import Order.*
import Item.*
import FileWriter.*
import GenParams.*

import java.util.Date
import java.nio.file.Path
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets

given s3Api: S3 = S3.at(Region.EU_CENTRAL_1)

extension(prod: Product)
  def toCSV(): String = prod.productIterator
    .map {
      case Some(value) => value
      case None        => ""
      case rest        => rest
    }
    .mkString(",")

object GenParams:
  val itemIdGen = Gen.choose(1, 1000)
  val yearGen = Gen.const(2021)
  val monthGen = Gen.choose(7, 7)
  val dayMin = 20
  val dayMax = 25
  val dayGen = Gen.choose(dayMin, dayMax)
  val batchSizeGen = Gen.choose(100, 400)

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

object Order:
  val orderMax = 100000
  val orderIdGen = Gen.choose(1, orderMax)
  val customerIdGen = Gen.choose(1, 100)

  val quantityGen = Gen.choose(2, 20)

  val csvHeader =
    "orderId,customerId,itemId,quantity,year,month,day,last_update_time"

  def gen =
    for 
      orderId <- orderIdGen
      customerId <- customerIdGen
      itemId <- itemIdGen
      quantity <- quantityGen
      year <- yearGen
      month <- monthGen
      day <- dayGen
    yield Order(
      orderId,
      customerId,
      itemId,
      quantity,
      year,
      month,
      day,
      System.currentTimeMillis
    )


def genRecords[T](gen: Gen[T]): Gen[List[T]] =
  for 
    size <- batchSizeGen
    events <- Gen.listOfN(size, gen)
  yield events

case class Item(
    itemId: Int,
    name: String,
    price: Double,
    year: Int,
    month: Int,
    day: Int,
    lastUpdateTime: Long
)

object Item:
  // strGen generates a fixed length random string
  val strGen = (n: Int) => Gen.listOfN(n, Gen.alphaChar).map(_.mkString)

  val itemMax = 10000
  val nameGen = strGen(10)
  val priceGen = Gen.choose(0.1, 50.0)

  val csvHeader = "itemId,name,price,year,month,day,last_update_time"

  def gen = for 
    itemId <- itemIdGen
    name <- nameGen
    price <- priceGen
    year <- yearGen
    month <- monthGen
    day <- dayGen
  yield Item(
    itemId,
    name,
    price,
    year,
    month,
    day,
    System.currentTimeMillis
  )

case class S3FileCfg[T](
    filePrefix: String,
    csvHeader: String,
    gen: Gen[List[T]],
    inBucket: Bucket,
    targetBucketFolder: String
)

case class LocalFileCfg[T](
    filePrefix: String,
    csvHeader: String,
    gen: Gen[List[T]],
    dirPath: String
)

trait FileWriter[T <: Product, U[_]]:
  def putFile(putCfg: U[T]): List[T]

object FileWriter:
  given s3FileWriter[T <: Product]: FileWriter[T, S3FileCfg] with  
    def putFile(putCfg: S3FileCfg[T]) = putS3File[T](putCfg)
    
  given localFileWriter[T <: Product]: FileWriter[T, LocalFileCfg] with    
    def putFile(putCfg: LocalFileCfg[T]) = putLocalFile[T](putCfg)

private def recordsToStr[T<: Product](gen: Gen[List[T]], header: String) = 
  val records = gen.sample.getOrElse(List.empty[T])
  val text = (List(header) ++ records.map(_.toCSV())).mkString("\n")
  (text, records)

def putS3File[T <: Product](putCfg: S3FileCfg[T]): List[T] = 
  val (text, records) = recordsToStr(putCfg.gen, putCfg.csvHeader)
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

def putLocalFile[T <: Product](putCfg: LocalFileCfg[T]): List[T] = 
  val (text, records) = recordsToStr(putCfg.gen, putCfg.csvHeader)
  val fileName = s"${putCfg.filePrefix}_${System.currentTimeMillis}.csv"  
  val path = Paths.get(putCfg.dirPath, fileName)
  Files.createDirectories(Paths.get(putCfg.dirPath))
  Files.write(path, text.getBytes(StandardCharsets.UTF_8))
  records

def getInputBucket(targetBucket: String, region: Region = Region.EU_CENTRAL_1) =
  s3Api
    .bucket(targetBucket)
    .getOrElse(
      sys.error(
        s"Bucket '$targetBucket' is not found in $region region"
      )
    )

@main
def s3GenerateItems(
    targetBucket: String,    
    targetBucketFolder: String,
    delay: Long = 1000
) = 
  val inBucket = getInputBucket(targetBucket)
  val expectedCount = (dayMax - dayMin + 1) * itemMax
  val putCfg = S3FileCfg[Item](
    "items",
    Item.csvHeader,
    genRecords(Item.gen),
    inBucket,
    targetBucketFolder
  )

  generateEntities[Item, S3FileCfg](
    putCfg,
    r => r.itemId + "_" + r.day,
    delay,
    expectedCount
  )

@main
def s3GenerateOrders(
    targetBucket: String,    
    targetBucketFolder: String,
    delay: Long
) = {
  val inBucket = getInputBucket(targetBucket)
  val expectedCount = (dayMax - dayMin + 1) * orderMax
  val putCfg = S3FileCfg[Order](
    "orders",
    Order.csvHeader,
    genRecords(Order.gen),
    inBucket,
    targetBucketFolder
  )

  generateEntities[Order, S3FileCfg](
    putCfg,
    r => r.orderId + "_" + r.day,
    delay,
    expectedCount
  )
}

private def generateEntities[T <: Product, U[_]](
    putCfg: U[T],
    uniqueId: T => String,
    delay: Long,
    expectedCount: Long
)(implicit writer: FileWriter[T, U]) = 
  var uniqueEntities = Set.empty[String]
  var count = 0

  Iterator
    .continually(writer.putFile(putCfg))
    .takeWhile(_ => uniqueEntities.size < expectedCount)
    .foreach { recs =>
      count += recs.length
      uniqueEntities ++= recs.map(uniqueId).toSet
      print(
        s"[${new Date()}] sent unique current / unique expected / total records: ${uniqueEntities.size} / $expectedCount / $count\r"
      )
      Thread.sleep(delay)
    }

@main
def localGenerateOrders(
    localGenPath: String,    
    delay: Long
) = 
  val expectedCount = (dayMax - dayMin + 1) * orderMax
  val putCfg = LocalFileCfg[Order](
    "orders",
    Order.csvHeader,
    genRecords(Order.gen),    
    localGenPath
  )

  generateEntities[Order, LocalFileCfg](
    putCfg,
    r => r.orderId + "_" + r.day,
    delay,
    expectedCount
  )

@main
def localGenerateItems(
    localGenPath: String,    
    delay: Long
) = 
  val expectedCount = (dayMax - dayMin + 1) * itemMax
  val putCfg = LocalFileCfg[Item](
    "items",
    Item.csvHeader,
    genRecords(Item.gen),
    localGenPath    
  )

  generateEntities[Item, LocalFileCfg](
    putCfg,
    r => r.itemId + "_" + r.day,
    delay,
    expectedCount
  )

