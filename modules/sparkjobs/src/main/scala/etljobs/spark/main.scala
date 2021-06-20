package etljobs.spark

import mainargs.{main, ParserForMethods}
import etljobs.common.FsUtil._
import etljobs.common.FileCopyParams
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

object FileToDataset extends App {
  @main
  def run(params: FileCopyParams) =
    sparkCopy(params)

  def sparkCopy(params: FileCopyParams) = {
    val output = targetDir(
      params.outputPath,
      JobContext(params.dagId, params.executionDate)
    )
    val sparkSession = SparkSession.builder.getOrCreate()

    useResource(sparkSession) { spark =>
      val inputData =
        spark.read
          .format("csv")
          .option("pathGlobFilter", params.globPattern)
          .load(params.inputPath.toString())
      println(s"output path: $output")
      inputData.write.mode(SaveMode.Overwrite).csv(output.toString())

      // move source files to processed directory
      params.processedDir.foreach { dst =>
        val fs = FileSystem.get(new Configuration())
        val srcFiles = listFiles(params.globPattern, params.inputPath)
        srcFiles.foreach { src =>
          moveFile(src, dst, fs)
        }
      }
    }
  }

  def useResource[T <: AutoCloseable](r: T)(f: T => Unit) =
    try f(r)
    finally r.close()

  ParserForMethods(this).runOrExit(args)
}
