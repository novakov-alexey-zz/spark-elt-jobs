package etljobs.common

import mainargs.TokensReader
import java.time.LocalDate
import java.net.URI

object MainArgsUtil {
  implicit object UriRead
      extends TokensReader[URI](
        "uri",
        strs => Right(new URI(strs.head))
      )

  implicit object DateRead
      extends TokensReader[LocalDate](
        "localDate",
        strs => Right(LocalDate.parse(strs.head))
      )
}
