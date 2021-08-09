import $file.nextDate

import scala.sys.process._

@main
def triggerDag(
    @arg(doc = "EMR cluster ID, for example j-1OPLJEOJYBWHL") clusterId: String
) = {
  val filePath = os.pwd / "scripts" / "execution-date.txt"
  val dagId = "spark_emr_hudi"
  val conf = ujson.write(Map("cluster_id" -> clusterId))

  nextDate.moveExecutionDate(filePath) { executionDate =>
    s"airflow dags trigger --exec-date $executionDate $dagId -c '$conf'".!
  }
}
