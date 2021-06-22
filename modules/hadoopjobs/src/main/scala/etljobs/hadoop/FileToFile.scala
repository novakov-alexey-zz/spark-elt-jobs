package etljobs.hadoop

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path => HPath}
import mainargs.{main, ParserForMethods}
import etljobs.common.FileCopyCfg
import etljobs.common.FsUtil.{listFiles, contextDir, moveFile}
import etljobs.common.FsUtil.JobContext
import java.io.File

object FileToFile extends App {
  @main
  def run(params: FileCopyCfg) =
    hadoopCopy(params)

  def hadoopCopy(cfg: FileCopyCfg) = {
    val srcFiles = cfg.entityPatterns.foldLeft(Array.empty[File]) {
      (acc, p) =>
        acc ++ listFiles(p.globPattern, cfg.inputPath)
    }
    println(s"Found files:\n${srcFiles.mkString("\n")}")

    val output = contextDir(
      cfg.outputPath,
      JobContext(cfg.dagId, cfg.executionDate)
    )
    lazy val fs = FileSystem.get(new Configuration())
    srcFiles.foreach { src =>
      val destPath = new HPath(output.resolve(src.getName()).toString())
      fs.delete(destPath, false)
      FileUtil.copy(src, fs, destPath, false, fs.getConf())

      cfg.processedDir.foreach { dest =>
        moveFile(src, dest, fs)
      }
    }
  }

  ParserForMethods(this).runOrExit(args)
}
