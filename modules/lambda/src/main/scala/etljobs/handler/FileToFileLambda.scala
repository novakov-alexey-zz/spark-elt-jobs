package etljobs.handler

import etljobs.hadoop.FileToFile
import etljobs.common.FileCopyCfg

import io.circe.parser.decode
import io.circe.generic.auto._
import io.circe.Decoder
import com.amazonaws.services.lambda.runtime.Context
import mainargs.Flag

import java.io.{InputStream, OutputStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.net.URI

import scala.util.Try
import scala.io.Source

class FileToFileLambda {

  implicit val decodeUri: Decoder[URI] = Decoder.decodeString.emapTry { str =>
    Try(new URI(str))
  }

  implicit val decodeOverwriteFlag: Decoder[Flag] =
    Decoder.decodeString.emapTry { str =>
      Try(Flag(str.toBoolean))
    }

  def handleRequest(
      in: InputStream,
      out: OutputStream,
      context: Context
  ): Unit = {
    val payload = Source.fromInputStream(in).mkString("")

    println(s"Event recieved: $payload")
    println(s"Req id: ${context.getAwsRequestId()}")
    val cfg = decode[FileCopyCfg](payload)
    cfg match {
      case Left(value) =>
        throw value.fillInStackTrace()
      case Right(copyCfg) =>
        FileToFile.hadoopCopy(copyCfg)
        out.write("OK".getBytes(UTF_8))
    }
  }
}
