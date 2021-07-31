import $file.nextDate

import scala.sys.process._

val filePath = os.pwd / "scripts" / "execution-date.txt"
val dagId = "spark_hudi"

nextDate.moveExecutionDate(filePath) { executionDate =>
  s"airflow dags trigger --exec-date $executionDate $dagId".!
}
