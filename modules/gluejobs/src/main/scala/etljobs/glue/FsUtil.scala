package etljobs.glue

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

import java.net.URI

object FsUtil {

  private def listFiles(
      conf: Configuration,
      globPattern: String,
      inputPath: URI
  ): Array[URI] = {
    val fs = FileSystem.get(inputPath, conf)
    val path = new Path(s"$inputPath/$globPattern")
    val statuses = fs.globStatus(path)
    statuses.map(_.getPath.toUri())
  }

  private def moveFile(
      src: URI,
      destinationDir: URI,
      conf: Configuration
  ): Boolean = {
    val fileName = src.toString
      .split("/")
      .lastOption
      .getOrElse(
        sys.error(s"Failed to get file name from the path: $src")
      )
    val destPath = new Path(s"$destinationDir/$fileName")
    val srcFs = FileSystem.get(src, conf)
    val srcPath = new Path(src.toString)
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

  def moveFiles(
      conf: Configuration,
      entityPatterns: List[EntityPattern],
      processedDir: Option[URI],
      input: URI
  ): Unit = {
    val dest =
      processedDir.getOrElse(new URI(s"$input/processed"))
    entityPatterns.foreach { p =>
      val srcFiles = listFiles(conf, p.globPattern, input)
      println(s"moving files: ${srcFiles.mkString(",\n")}\nto $dest")
      srcFiles.foreach(src => moveFile(src, dest, conf))
    }
  }
}
