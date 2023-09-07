import findspark
findspark.init()
import os
import shutil
os.environ ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 pyspark-shell'
from pyspark.sql import SparkSession
from InputStreamHandler import InputStreamHandler
from ApplySchema import ApplySchema
from WriteStream import WriteStream
from Aggregate import Aggregate


dir_path = "spark-warehouse/trend_table"
if os.path.exists(dir_path) and os.path.isdir(dir_path):
    shutil.rmtree(dir_path)

spark_wondow_duration = "2 minutes"

# Create a SparkSession
spark = SparkSession.builder.appName("TrendAnalysis").getOrCreate()

# Initialize classes and process the stream
input_handler = InputStreamHandler(spark, "meetup_events")
streaming_df = input_handler.create_input_stream_dataframe()

apply_schema = ApplySchema(spark)
streaming_df = apply_schema.infer_and_apply_schema(streaming_df)

aggregate = Aggregate(streaming_df, spark_wondow_duration)
aggregate_df = aggregate.get_aggregated_df()

write_stream = WriteStream(spark, "meetup_events")

# Set the trigger to process the stream every 2 minutes
write_stream.process_batch(aggregate_df).trigger(processingTime=spark_wondow_duration).start()

# Start the Spark Streaming query
spark.streams.awaitAnyTermination()
