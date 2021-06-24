package etljobs.spark

import etljobs.common.FsUtil._
import etljobs.common.EntityPattern

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

import java.nio.file.Path
import scala.io.Source
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructType
import etljobs.common.FileCopyCfg

object common {

  def useResource[T <: AutoCloseable](r: T)(f: T => Unit) =
    try f(r)
    finally r.close()

  def requireMove(cfg: SparkCopyCfg) =
    cfg.moveFiles.value || cfg.fileCopy.processedDir.isDefined

  def moveFiles(
      entityPatterns: List[EntityPattern],
      processedDir: Option[Path],
      input: Path
  ) = {
    val dest =
      processedDir.getOrElse(input.resolve("processed"))
    val fs = FileSystem.get(new Configuration())
    entityPatterns.foreach { p =>
      val srcFiles =
        listFiles(p.globPattern, input)
      println(s"moving files: ${srcFiles.mkString(",\n")} to $dest")
      srcFiles.foreach(src => moveFile(src, dest, fs))
    }
  }

  def getSchema(schemaPath: Path, entityName: String): StructType = {
    val jsonSchema =
      Source
        .fromFile(schemaPath.resolve(s"$entityName.json").toFile())
        .getLines
        .mkString
    DataType.fromJson(jsonSchema).asInstanceOf[StructType]
  }

  def getInOutPaths(fileCopy: FileCopyCfg) = {
    val context =
      JobContext(
        fileCopy.dagId,
        fileCopy.executionDate
      )
    val output = contextDir(fileCopy.outputPath, context)
    val input = contextDir(fileCopy.inputPath, context)
    (input, output)
  }
}
