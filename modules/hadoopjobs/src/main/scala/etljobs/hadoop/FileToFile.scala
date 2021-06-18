package etljobs.hadoop

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path => HPath}
import mainargs.{main, ParserForMethods}
import etljobs.common.FileCopyParams
import etljobs.common.FsUtil.{sourceFiles, targetDir, moveFile}
import etljobs.common.FsUtil.JobContext

object FileToFile extends App {
  @main
  def run(params: FileCopyParams) =
    hadoopCopy(params)

  def hadoopCopy(params: FileCopyParams) = {
    val fs = FileSystem.get(new Configuration())
    val srcFiles = sourceFiles(params.globPattern, params.inputPath)
    println(s"Found files:\n${srcFiles.mkString("\n")}")

    val output = targetDir(
      params.outputPath,
      JobContext(params.dagId, params.executionDate)
    )
    srcFiles.foreach { src =>
      val destPath = new HPath(output.resolve(src.getName()).toString())
      fs.delete(destPath, false)
      FileUtil.copy(src, fs, destPath, false, fs.getConf())

      if (params.moveSourceFiles.value)
        moveFile(src, params.processedDir, fs)
    }
  }

  ParserForMethods(this).runOrExit(args)
}
