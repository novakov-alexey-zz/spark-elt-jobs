import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import scala.sys.process._

def moveExecutionDate[A](filePath: os.Path)(cb: String => A): A = {
  val defaultDate = LocalDateTime.of(2021, 6, 21, 11, 2, 52)

  val currentDate = if (os.exists(filePath)) {
    val rawDate = os.read(filePath).trim
    LocalDateTime.parse(rawDate)
  } else defaultDate

  val nextDate = currentDate.plusSeconds(1)
  val executionDate = nextDate.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
  
  val res = cb(executionDate)
  os.write.over(filePath, executionDate)
  res
}
