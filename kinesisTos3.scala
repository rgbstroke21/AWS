import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.MappingSpec
import com.amazonaws.services.glue.errors.CallSite
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import com.amazonaws.services.glue.DynamicFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.streaming.Trigger
import java.util.Calendar

object GlueApp {
  def main(sysArgs: Array[String]) {
    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)
    // @params: [JOB_NAME]
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    // Script generated for node Kinesis Stream
    val dataframe_KinesisStream_node1 = glueContext.getSource(connectionType="kinesis",connectionOptions={JsonOptions("""{"typeOfData": "kinesis", "streamARN": "arn:aws:kinesis:us-east-1:530992065438:stream/dojostream", "classification": "json", "startingPosition": "earliest", "inferSchema": "false"}""")}, transformationContext="dataframe_KinesisStream_node1").getDataFrame()

    glueContext.forEachBatch(dataframe_KinesisStream_node1, (dataFrame: Dataset[Row], batchId: Long) => {
      if (dataFrame.count() > 0) {
        val KinesisStream_node1 = DynamicFrame(dataFrame, glueContext)
        // Script generated for node ApplyMapping
        val ApplyMapping_node2 = KinesisStream_node1.selectFields(paths=Seq(), transformationContext="ApplyMapping_node2")

        val year: Int = Calendar.getInstance().get(Calendar.YEAR)
        val month: Int = Calendar.getInstance().get(Calendar.MONTH) + 1
        val day: Int = Calendar.getInstance().get(Calendar.DATE)
        val hour: Int = Calendar.getInstance().get(Calendar.HOUR_OF_DAY)

        // Script generated for node S3 bucket
        val S3bucket_node3_path = "s3://dojo-data-stream2345/output" + "/ingest_year=" + "%04d".format(year) + "/ingest_month=" + "%02d".format(month) + "/ingest_day=" + "%02d".format(day) + "/ingest_hour=" + "%02d".format(hour) + "/"
        val S3bucket_node3 = glueContext.getSinkWithFormat(connectionType="s3", options=JsonOptions("""{"path": """" + S3bucket_node3_path + """", "partitionKeys": []}"""), transformationContext="S3bucket_node3", format="csv").writeDynamicFrame(ApplyMapping_node2)

      }
    }, JsonOptions(s"""{"windowSize" : "100 seconds", "checkpointLocation" : "${args("TempDir")}/${args("JOB_NAME")}/checkpoint/"}"""))
    Job.commit()
  }
}