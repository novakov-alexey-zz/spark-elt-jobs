package etljobs.common

import org.apache.hadoop.fs.{FileSystem, FileUtil, Path => HPath}
import org.apache.hadoop.fs.GlobFilter
import java.nio.file.Path
import java.io.File
import java.time.LocalDate

object FsUtil {

  def listFiles(globPattern: String, inputPath: Path) = {
    val filter = new GlobFilter(globPattern)
    val dir = inputPath.toFile()
    FileUtil
      .listFiles(dir)
      .filter(f => filter.accept(new HPath(f.toString())))
  }

  def moveFile(src: File, destinationDir: Path, fs: FileSystem) = {
    val processedPath = new HPath(
      destinationDir.resolve(src.getName()).toString()
    )
    FileUtil.copy(src, fs, processedPath, true, fs.getConf())
  }

  case class JobContext(dagId: String, executionDate: LocalDate)

  def targetDir(rootDir: Path, ctx: JobContext) =
    Path.of(
      rootDir.toString(),
      ctx.dagId,
      ctx.executionDate.toString
    )
}
