from pyspark.sql import DataFrame
from pyspark.sql.functions import window, col, count

class Aggregate:
    def __init__(self, stream_df, window_duration):
        self.stream_df = stream_df
        self.window_duration = window_duration

    def get_aggregated_df(self):
        streaming_df = self.stream_df.withWatermark("EventDate", "1 minutes")

        # Create a windowed DataFrame
        windowed_df = streaming_df \
            .groupBy(
                window(col("EventDate"), self.window_duration),
                col("Topic_Name")
            )

        aggregated_df = windowed_df \
            .agg(count("Topic_Name").alias("Topic_Count"))

        return aggregated_df
