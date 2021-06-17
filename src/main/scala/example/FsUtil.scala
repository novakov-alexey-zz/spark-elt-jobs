package example

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path => HPath}
import org.apache.hadoop.fs.GlobFilter
import org.apache.spark.sql.SaveMode
import java.nio.file.Path
import java.io.File

object FsUtil {
  def hadoopCopy(params: Params) = {
    val fs = FileSystem.get(new Configuration())
    val srcFiles = sourceFiles(params)

    val output = outputDir(params)
    srcFiles.foreach { src =>
      val destPath = new HPath(output.resolve(src.getName()).toString())
      FileUtil.copy(src, fs, destPath, false, fs.getConf())

      if (params.moveSourceFiles.value) {
        moveFile(src, params.processedDir, fs)
      }
    }
  }

  def sparkCopy(params: Params) = {
    val output = outputDir(params)
    val sparkSession =
      SparkSession.builder
        .appName("FileToDataset")
        .getOrCreate()

    useResource(sparkSession) { spark =>
      val inputData =
        spark.read
          .format("csv")
          .option("pathGlobFilter", params.globPattern)
          .load(params.inputPath.toString())
      println(s"output path: $output")
      inputData.write.mode(SaveMode.Overwrite).csv(output.toString())

      if (params.moveSourceFiles.value) {
        val fs = FileSystem.get(new Configuration())
        val srcFiles = sourceFiles(params)
        srcFiles.foreach { src =>
          moveFile(src, params.processedDir, fs)
        }
      }
    }
  }

  private def sourceFiles(params: Params) = {
    val filter = new GlobFilter(params.globPattern)
    FileUtil
      .listFiles(params.inputPath.toFile())
      .filter(f => filter.accept(new HPath(f.toString())))
  }

  private def moveFile(src: File, processedDir: Path, fs: FileSystem) = {
    val processedPath = new HPath(
      processedDir.resolve(src.getName()).toString()
    )
    FileUtil.copy(src, fs, processedPath, true, fs.getConf())
  }

  private def outputDir(params: Params) =
    Path.of(
      params.outputPath.toString(),
      params.dagId,
      params.taskId,
      params.executionDate.toString
    )

  private def useResource[T <: AutoCloseable](r: T)(f: T => Unit) =
    try f(r)
    finally r.close()
}
