import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.MappingSpec
import com.amazonaws.services.glue.errors.CallSite
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
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

    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)

    import glueContext.sparkSession.implicits._

    val kinesisDataStream = glueContext.readStream
    .format("kinesis")
    .option("streamName", "dojostream" )
    .option("endPointUrl", "https://kinesis.us-east-1.amazonaws.com")
    .option("startingPosition", "TRIM_HORIZON")
    .load()
    

    println("Reading done.. Printing schema...")
    kinesisDataStream.printSchema()
    
    // Deserialization
    val deserializedData = kinesisDataStream.select(
        $"data".cast("string"),
        $"partitionKey",
        $"approximateArrivalTimestamp"
    )
    
    val finalData = deserializedData
    .withColumn("year", year(col("approximateArrivalTimestamp")))
    .withColumn("month", month(col("approximateArrivalTimestamp")))
    .withColumn("day", dayofmonth(col("approximateArrivalTimestamp")))
    .withColumn("hour", hour(col("approximateArrivalTimestamp")))
    .drop("approximateArrivalTimestamp")
    
    /*
    var finalData : DataFrame = kinesisDataStream.select(
        from_json(
            $"partitionKey".cast("string"),
            StructType.fromDDL(
                """
                    |partitionKey STRING
                    |""".stripMargin)
        ) as "pk",
        from_json(
            $"data".cast("string"),
            StructType.fromDDL(
                """
                    |col1 STRING,
                    |col2 STRING,
                    |col3 LONG
                    |""".stripMargin)
        ) as "data"
    ).select( col("data.col1") , col("data.col2") , col("pk.partitionKey").alias("aws_account_id") , col("data.col3") )
    */
    
    
    
    val checkPoint = "s3://dojo-data-stream2345/checkPoint"
    val outputPath = "s3://dojo-data-stream2345/output/"

    
    def processBatch( df : DataFrame , batchId : Long ): Unit = {
        println(s"Processing BatchId : $batchId")
        df.show(100 , truncate = false)
        df.write.partitionBy("year","month","day","hour").mode(SaveMode.Append).parquet(outputPath)
    }

    glueContext.forEachBatch(
        dataFrame = finalData,
        writeStreamFunction = processBatch,
        options = JsonOptions(s"""{"windowSize": "5 seconds", "checkpointLocation": "$checkPoint"}""") 
    )
    
    Job.commit()
  }
}
