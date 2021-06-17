package example

import mainargs.{main, ParserForMethods}

object FileToFile extends App {
  @main
  def run(params: Params) =
    FsUtil.hadoopCopy(params)

  ParserForMethods(this).runOrExit(args)
}

object FileToDataset extends App {
  @main
  def run(params: Params) =
    FsUtil.sparkCopy(params)

  ParserForMethods(this).runOrExit(args)
}
