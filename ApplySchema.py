from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType, TimestampType, ArrayType

class ApplySchema:
    def __init__(self, sparkSession):
        self.sparkSession = sparkSession

    def infer_and_apply_schema(self, streaming_df):
        schema = self.define_schema()
        streaming_df = streaming_df.withColumn("value_str", col("value").cast(StringType()))

        streaming_df = streaming_df \
            .withColumn("parsed_event", from_json(col("value_str"), schema)) \
            .select("parsed_event.*", "timestamp")

        stream_with_timestamp = streaming_df \
            .withColumn("EventDate", to_timestamp(col("timestamp"), "yyyyMMddHHmmss")) \
            .select(explode(col("group.group_topics.topic_name")).alias("Topic_Name"),
                    col("group.group_city").alias("Group_City"),
                    col("group.group_country").alias("Group_Country"),
                    col("EventDate"))

        return stream_with_timestamp

    def define_schema(self):
        schema = StructType([
            StructField("venue", StructType([
                StructField("venue_name", StringType(), True),
                StructField("lon", DoubleType(), True),
                StructField("lat", DoubleType(), True),
                StructField("venue_id", LongType(), True)
            ]), True),
            StructField("visibility", StringType(), True),
            StructField("response", StringType(), True),
            StructField("guests", IntegerType(), True),
            StructField("member", StructType([
                StructField("member_id", LongType(), True),
                StructField("photo", StringType(), True),
                StructField("member_name", StringType(), True)
            ]), True),
            StructField("rsvp_id", LongType(), True),
            StructField("mtime", TimestampType(), True),
            StructField("event", StructType([
                StructField("event_name", StringType(), True),
                StructField("event_id", StringType(), True),
                StructField("time", TimestampType(), True),
                StructField("event_url", StringType(), True)
            ]), True),
            StructField("group", StructType([
                StructField("group_topics", ArrayType(StructType([
                    StructField("urlkey", StringType(), True),
                    StructField("topic_name", StringType(), True)
                ])), True),
                StructField("group_city", StringType(), True),
                StructField("group_country", StringType(), True),
                StructField("group_id", LongType(), True),
                StructField("group_name", StringType(), True),
                StructField("group_lon", DoubleType(), True),
                StructField("group_urlname", StringType(), True),
                StructField("group_lat", DoubleType(), True)
            ]), True)
        ])

        return schema

