package etljobs.glue

import java.net.URI
import java.time.LocalDate

case class EntityPattern(
    name: String,
    globPattern: String,
    dedupKey: Option[String]
)

case class SparkOption(name: String, value: String)

case class JobContext(executionDate: LocalDate, jobId: String)

case class JobConfig(
    ctx: JobContext,
    triggerInterval: Long, // negative value means Trigger.Once
    inputFormat: String,
    saveFormat: String,
    inputPath: URI,
    outputPath: URI,
    hadoopConfig: List[SparkOption],
    readerOptions: List[SparkOption],
    schemaPath: URI,
    partitionBy: List[String],
    moveFiles: Boolean,
    entityPatterns: List[EntityPattern],
    processedDir: Option[URI] = None
)
