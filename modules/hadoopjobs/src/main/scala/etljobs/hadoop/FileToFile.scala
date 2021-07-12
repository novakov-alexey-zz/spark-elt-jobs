package etljobs.hadoop

import etljobs.common.{FileCopyCfg, HadoopCfg}
import etljobs.common.FsUtil.{listFiles, contextDir, moveFile, JobContext}

import org.apache.hadoop.fs.{FileSystem, FileUtil, Path => HPath}
import mainargs.{main, ParserForMethods}

import java.nio.file.Path
import java.net.URI

object FileToFile extends App {
  @main
  def run(params: FileCopyCfg) =
    hadoopCopy(params)

  def hadoopCopy(cfg: FileCopyCfg) = {
    lazy val conf = HadoopCfg.get(cfg.hadoopConfig)
    val srcFiles = cfg.entityPatterns.foldLeft(List.empty[URI]) { (acc, p) =>
      acc ++ listFiles(conf, p.globPattern, cfg.inputPath)
    }
    val foundFiles =
      if (srcFiles.nonEmpty) "\n" + srcFiles.mkString("\n")
      else "<empty list>"
    println(s"Found files: ${foundFiles}")

    val output = contextDir(
      cfg.outputPath,
      JobContext(cfg.ctx.jobId, cfg.ctx.executionDate)
    )

    lazy val destFs = FileSystem.get(output, conf)
    srcFiles.foreach { src =>
      val srcFs = FileSystem.get(src, conf)
      val srcPath = new HPath(src.toString())
      val fileName = Path.of(src.getPath()).getFileName.toString()
      val destPath = new HPath(s"$output/$fileName")

      FileSystem.get(output, conf).delete(destPath, false)
      FileUtil.copy(srcFs, srcPath, destFs, destPath, false, conf)

      cfg.processedDir.foreach { suffix =>
        val output = new URI(s"${cfg.inputPath}/$suffix")
        moveFile(src, output, conf)
      }
    }
  }

  ParserForMethods(this).runOrExit(args)
}
