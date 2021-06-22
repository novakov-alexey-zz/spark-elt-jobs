package etljobs.common

import org.apache.hadoop.fs.{FileSystem, FileUtil, Path => HPath, GlobFilter}
import java.nio.file.Path
import java.io.File
import java.time.LocalDate

object FsUtil {

  def listFiles(globPattern: String, inputPath: Path): Array[File] = {
    val dir = inputPath.toFile()
    if (dir.exists()) {
      val filter = new GlobFilter(globPattern)
      FileUtil
        .listFiles(dir)
        .filter(f => filter.accept(new HPath(f.toString())))
    } else
      Array.empty[File]
  }

  def moveFile(src: File, destinationDir: Path, fs: FileSystem): Boolean = {
    val processedPath = new HPath(
      destinationDir.resolve(src.getName()).toString()
    )
    FileUtil.copy(src, fs, processedPath, true, fs.getConf())
  }

  case class JobContext(dagId: String, executionDate: LocalDate)

  def contextDir(rootDir: Path, ctx: JobContext): Path =
    Path.of(
      rootDir.toString(),
      ctx.dagId,
      ctx.executionDate.toString
    )
}
