package etljobs.hadoop

import etljobs.common.{FileCopyCfg, HadoopCfg}
import etljobs.common.FsUtil.{listFiles, contextDir, moveFile}
import etljobs.common.FsUtil.JobContext

import org.apache.hadoop.fs.{FileSystem, FileUtil, Path => HPath}
import mainargs.{main, ParserForMethods}

import java.nio.file.Path
import java.net.URI

object FileToFile extends App {
  @main
  def run(params: FileCopyCfg) =
    hadoopCopy(params)

  def hadoopCopy(cfg: FileCopyCfg) = {
    val conf = HadoopCfg.get(cfg.hadoopConfig)
    println(cfg.hadoopConfig.mkString("\n"))
    val srcFiles = cfg.entityPatterns.foldLeft(Array.empty[URI]) { (acc, p) =>
      acc ++ listFiles(conf, p.globPattern, cfg.inputPath)
    }
    val foundFiles =
      if (srcFiles.nonEmpty) srcFiles.mkString("\n") else "<empty list>"
    println(s"Found files:\n${foundFiles}")

    val output = contextDir(
      cfg.outputPath,
      JobContext(cfg.dagId, cfg.executionDate)
    )

    srcFiles.foreach { src =>
      val srcFs = FileSystem.get(src, conf)
      val srcPath = new HPath(src.toString())
      val fileName = Path.of(src.getPath()).getFileName.toString()
      val destPath = new HPath(output.resolve(fileName).toString())
      val destFs = FileSystem.get(output, conf)

      FileSystem.get(output, conf).delete(destPath, false)
      FileUtil.copy(srcFs, srcPath, destFs, destPath, false, conf)

      cfg.processedDir.foreach { dest =>
        moveFile(src, dest, conf)
      }
    }
  }

  ParserForMethods(this).runOrExit(args)
}
