package etljobs.common

import org.apache.hadoop.fs.{FileSystem, FileUtil, Path => HPath}
import org.apache.hadoop.fs.GlobFilter
import java.nio.file.Path
import java.io.File

object FsUtil {

  def sourceFiles(globPattern: String, inputPath: Path) = {
    val filter = new GlobFilter(globPattern)
    FileUtil
      .listFiles(inputPath.toFile())
      .filter(f => filter.accept(new HPath(f.toString())))
  }

  def moveFile(src: File, processedDir: Path, fs: FileSystem) = {
    val processedPath = new HPath(
      processedDir.resolve(src.getName()).toString()
    )
    FileUtil.copy(src, fs, processedPath, true, fs.getConf())
  }

  def outputDir(params: Params) =
    Path.of(
      params.outputPath.toString(),
      params.dagId,
      params.taskId,
      params.executionDate.toString
    )
}
