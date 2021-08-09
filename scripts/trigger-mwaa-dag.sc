import $ivy.`com.amazonaws:aws-java-sdk-mwaa:1.12.33`
import $file.nextDate

import java.util.Base64
import com.amazonaws.regions.Regions
import com.amazonaws.services.mwaa.AmazonMWAAClient
import com.amazonaws.services.mwaa.model.CreateCliTokenResult
import com.amazonaws.services.mwaa.model.CreateCliTokenRequest

val region = Regions.EU_CENTRAL_1
val mwaaEnvName = "MyAirflowEnvironment"
val dagName = "spark_emr_hudi"
val executionDate = "2021-07-25T11:05:54"

@main
def triggerDag(
    @arg(doc = "EMR cluster ID, for example j-1OPLJEOJYBWHL") clusterId: String
) = {
  val mwaa = AmazonMWAAClient.builder.withRegion(region).build
  val response =
    mwaa.createCliToken(new CreateCliTokenRequest().withName(mwaaEnvName))
  val token = response.getCliToken
  val hostName = response.getWebServerHostname

  val mwaaAuthToken = "Bearer " + token
  val mwaaWebserverHostname = s"https://$hostName/aws_mwaa/cli"
  val conf = ujson.write(Map("cluster_id" -> clusterId))
  val rawData = (executionDate: String) =>
    s"dags trigger --exec-date $executionDate $dagName -c '$conf'"

  val fileName = os.pwd / "scripts" / "aws-execution-date.txt"

  val mwaaResponse = nextDate.moveExecutionDate(fileName) { execDate =>
    requests.post(
      mwaaWebserverHostname,
      headers = Seq(
        "Authorization" -> mwaaAuthToken,
        "Content-Type" -> "text/plain"
      ),
      data = rawData(execDate),
      readTimeout = 20000,
      connectTimeout = 20000
    )
  }

  val reply = ujson.read(mwaaResponse.text).obj.toMap
  val error = reply.get("stderr").map(_.str)
  val out = reply.get("stdout").map(_.str)

  def decodeAndPrint(value: Option[String]) =
    value.foreach { v =>
      println(new String(Base64.getDecoder.decode(v)))
    }

  println(mwaaResponse.statusCode)
  decodeAndPrint(error)
  decodeAndPrint(out)
}
