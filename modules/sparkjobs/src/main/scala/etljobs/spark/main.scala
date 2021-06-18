package etljobs.spark

import mainargs.{main, ParserForMethods}
import etljobs.common.FsUtil._
import etljobs.common.Params
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

object FileToDataset extends App {
  @main
  def run(params: Params) =
    sparkCopy(params)

  def sparkCopy(params: Params) = {
    val output = outputDir(params)
    val sparkSession = SparkSession.builder.getOrCreate()

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
        val srcFiles = sourceFiles(params.globPattern, params.inputPath)
        srcFiles.foreach { src =>
          moveFile(src, params.processedDir, fs)
        }
      }
    }
  }

  def useResource[T <: AutoCloseable](r: T)(f: T => Unit) =
    try f(r)
    finally r.close()

  ParserForMethods(this).runOrExit(args)
}
