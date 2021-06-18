package etljobs.common

import mainargs.TokensReader
import java.time.LocalDate
import java.nio.file.Path

object MainArgsUtil {
  implicit object PathRead
      extends TokensReader[Path](
        "path",
        strs => Right(Path.of(strs.head))
      )
  implicit object DateRead
      extends TokensReader[LocalDate](
        "localDate",
        strs => Right(LocalDate.parse(strs.head))
      )
}
