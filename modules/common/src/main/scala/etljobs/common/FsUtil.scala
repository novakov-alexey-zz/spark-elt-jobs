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
    val path = new HPath(s"$inputPath/$globPattern")
    val statuses = fs.globStatus(path)
    statuses.map(_.getPath.toUri())
  }

  def moveFile(src: URI, destinationDir: URI, conf: Configuration): Boolean = {
    val fileName = Path.of(src.toString).getFileName.toString
    val destPath = new HPath(s"$destinationDir/$fileName")
    val srcFs = FileSystem.get(src, conf)
    val srcPath = new HPath(src.toString)
    val destFs = FileSystem.get(destPath.toUri, conf)

    FileUtil.copy(
      srcFs,
      srcPath,
      destFs,
      destPath,
      true,
      conf
    )
  }

  case class JobContext(jobId: String, executionDate: LocalDate)

  def contextDir(rootDir: URI, ctx: JobContext): URI =
    new URI(
      s"$rootDir/${ctx.jobId}/${ctx.executionDate.toString}"
    )
}
