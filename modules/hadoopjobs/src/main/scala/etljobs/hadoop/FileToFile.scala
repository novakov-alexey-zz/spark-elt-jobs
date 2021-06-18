package etljobs.hadoop

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path => HPath}
import mainargs.main
import etljobs.common.Params
import etljobs.common.FsUtil.{sourceFiles, outputDir, moveFile}
import mainargs.ParserForMethods

object FileToFile extends App {
  @main
  def run(params: Params) =
    hadoopCopy(params)

  def hadoopCopy(params: Params) = {
    val fs = FileSystem.get(new Configuration())
    val srcFiles = sourceFiles(params.globPattern, params.inputPath)

    val output = outputDir(params)
    srcFiles.foreach { src =>
      val destPath = new HPath(output.resolve(src.getName()).toString())
      FileUtil.copy(src, fs, destPath, false, fs.getConf())

      if (params.moveSourceFiles.value)
        moveFile(src, params.processedDir, fs)
    }
  }

  ParserForMethods(this).runOrExit(args)
}
