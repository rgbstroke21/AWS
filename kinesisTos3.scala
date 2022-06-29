import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame, Row
import datetime
from awsglue import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [stream_type = kinesis, stream_batch_time = "100 seconds", database = "dojodatabase", additionalOptions = {"startingPosition": "TRIM_HORIZON", "inferSchema": "false"}, stream_checkpoint_location = "s3://dojo-data-stream2345/checkpoint/", table_name = "dojotable"]
## @return: datasource0
## @inputs: []
data_frame_datasource0 = glueContext.create_data_frame.from_catalog(database = "dojodatabase", table_name = "dojotable", transformation_ctx = "datasource0", additional_options = {"startingPosition": "TRIM_HORIZON", "inferSchema": "false"})
def processBatch(data_frame, batchId):
    if (data_frame.count() > 0):
        datasource0 = DynamicFrame.fromDF(data_frame, glueContext, "from_data_frame")
        ## @type: ApplyMapping
        ## @args: [mapping = [("sensor", "string", "sensor", "string"), ("temperature", "int", "temperature", "int"), ("vibration", "int", "vibration", "int")], transformation_ctx = "applymapping0"]
        ## @return: applymapping0
        ## @inputs: [frame = datasource0]
        applymapping0 = ApplyMapping.apply(frame = datasource0, mappings = [("sensor", "string", "sensor", "string"), ("temperature", "int", "temperature", "int"), ("vibration", "int", "vibration", "int")], transformation_ctx = "applymapping0")
        ## @type: DataSink
        ## @args: [stream_batch_time = "100 seconds", stream_checkpoint_location = "s3://dojo-data-stream2345/checkpoint/", connection_type = "s3", path = "s3://dojo-data-stream2345", format = "csv", transformation_ctx = "datasink1"]
        ## @return: datasink1
        ## @inputs: [frame = applymapping0]
        now = datetime.datetime.now()
        year = now.year
        month = now.month
        day = now.day
        hour = now.hour
        minute = now.minute
        path_datasink1 = "s3://dojo-data-stream2345" + "/ingest_year=" + "{:0>4}".format(str(year)) + "/ingest_month=" + "{:0>2}".format(str(month)) + "/ingest_day=" + "{:0>2}".format(str(day)) + "/ingest_hour=" + "{:0>2}".format(str(hour)) + "/"
        datasink1 = glueContext.write_dynamic_frame.from_options(frame = applymapping0, connection_type = "s3", connection_options = {"path": path_datasink1}, format = "csv", transformation_ctx = "datasink1")
glueContext.forEachBatch(frame = data_frame_datasource0, batch_function = processBatch, options = {"windowSize": "100 seconds", "checkpointLocation": "s3://dojo-data-stream2345/checkpoint/"})
job.commit()