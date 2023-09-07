from pyspark.sql import SparkSession

class InputStreamHandler:
    def __init__(self, sparkSession, kafkaTopic):
        self.sparkSession = sparkSession
        self.kafkaTopic = kafkaTopic

    def create_input_stream_dataframe(self):
        return self.sparkSession \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", self.kafkaTopic) \
            .load()