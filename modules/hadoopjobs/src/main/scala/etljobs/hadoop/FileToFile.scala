package etljobs.hadoop

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path => HPath}
import mainargs.{main, ParserForMethods}
import etljobs.common.FileCopyParams
import etljobs.common.FsUtil.{listFiles, targetDir, moveFile}
import etljobs.common.FsUtil.JobContext
import java.io.File

object FileToFile extends App {
  @main
  def run(params: FileCopyParams) =
    hadoopCopy(params)

  def hadoopCopy(params: FileCopyParams) = {
    val srcFiles = params.entityPatterns.foldLeft(Array.empty[File]) {
      (acc, p) =>
        acc ++ listFiles(p.globPattern, params.inputPath)
    }
    println(s"Found files:\n${srcFiles.mkString("\n")}")

    val output = targetDir(
      params.outputPath,
      JobContext(params.dagId, params.executionDate)
    )
    lazy val fs = FileSystem.get(new Configuration())
    srcFiles.foreach { src =>
      val destPath = new HPath(output.resolve(src.getName()).toString())
      fs.delete(destPath, false)
      FileUtil.copy(src, fs, destPath, false, fs.getConf())

      params.processedDir.foreach { dest =>
        moveFile(src, dest, fs)
      }
    }
  }

  ParserForMethods(this).runOrExit(args)
}
