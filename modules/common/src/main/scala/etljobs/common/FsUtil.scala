package etljobs.common

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path => HPath}

import java.time.LocalDate
import java.net.URI
import java.nio.file.Path

object FsUtil {

  def listFiles(
      conf: Configuration,
      globPattern: String,
      inputPath: URI
  ): Array[URI] = {
    val fs = FileSystem.get(inputPath, conf)
    val path = new HPath(inputPath.resolve("/").resolve(globPattern).toString())
    println(s"list in path: $path")
    val statuses = fs.globStatus(path)
    statuses.map(_.getPath().toUri())
  }

  def moveFile(src: URI, destinationDir: URI, conf: Configuration): Boolean = {
    val fileName = Path.of(src.getPath()).getFileName
    val processedPath = new HPath(
      destinationDir.resolve(fileName.toString()).toString()
    )
    val srcFs = FileSystem.get(src, conf)
    val destFs = FileSystem.get(processedPath.toUri(), conf)
    FileUtil.copy(
      srcFs,
      new HPath(src.toString()),
      destFs,
      processedPath,
      true,
      conf
    )
  }

  case class JobContext(dagId: String, executionDate: LocalDate)

  def contextDir(rootDir: URI, ctx: JobContext): URI =
    rootDir.resolve(ctx.dagId).resolve(ctx.executionDate.toString)
}
