from pyspark.sql.functions import col, lit, pow, when, row_number, current_timestamp, count
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import sqrt, pow, lit



class WriteStream:
    def __init__(self, sparkSession, queryName):
        self.sparkSession = sparkSession
        self.queryName = queryName
        self.trend_decay = 0.8
        self.checkpointLocation = "./SC"
        self.trenOutputNumber = 20


    def process_batch(self, streamDF):
        query = streamDF.writeStream.foreachBatch(self._foreach_batch)
        return query


    def _foreach_batch(self, batchDF, batchId):
        if batchDF.head(1):
            batchDF.persist()
            table_names = self.sparkSession.catalog.listTables()

            if not any(table.name == 'trend_table' for table in table_names):
                if batchDF.groupBy("Topic_Name").count().filter(col("count") > 1).count() == 0:
                    processedDF = batchDF.withColumn("avg", col("Topic_Count").cast("decimal(38,3)")) \
                        .withColumn("sqr_Avg", pow(col("Topic_Count"), lit(2)).cast("decimal(38,3)"))

                    processedDF.select("Topic_Name", "avg", "sqr_Avg") \
                        .write.mode("overwrite").saveAsTable("trend_table")
            else:
                self.sparkSession.sparkContext.setCheckpointDir(
                    self.checkpointLocation
                )
                self.sparkSession.sql("refresh TABLE trend_table")
                trendDf = self.sparkSession.read.table("trend_table").checkpoint()
                joinedDF = batchDF.join(trendDf, "Topic_Name", "fullouter")

                processedDF = joinedDF.select(
                    when((col("avg") == 0) & (col("sqr_Avg") == 0), col("Topic_Count"))
                    .otherwise(col("avg") * lit(self.trend_decay) +
                               col("Topic_Count") * lit(1 - self.trend_decay))
                    .alias("avg"),
                    when((col("avg") == 0) & (col("sqr_Avg") == 0), pow(col("Topic_Count"), lit(2)).cast("decimal(38,3)"))
                    .otherwise(col("sqr_Avg") * lit(self.trend_decay) +
                               pow(col("Topic_Count"), lit(2)).cast("decimal(38,3)") * lit(1 - self.trend_decay))
                    .alias("sqr_Avg"),
                    "Topic_Name",
                    "Topic_Count"
                )

                processedDF.select("Topic_Name", "avg", "sqr_Avg") \
                    .write.mode("overwrite").saveAsTable("trend_table")

                finalDF = processedDF.select("Topic_Name",
                                             when(sqrt(pow((col("avg") - col("sqr_Avg")), lit(2))).cast("decimal(38,3)") == lit(0).cast("decimal(38,3)"),
                                                  col("Topic_Count") - col("avg"))
                                             .otherwise((col("Topic_Count") - col("avg")) / sqrt(pow((col("avg") - col("sqr_Avg")), lit(2)))).alias("Trend_Score")
                                             )

                windowSpec = Window.orderBy(col("Trend_Score").desc())
                trendDF = finalDF.withColumn("Trend_Rank", row_number().over(windowSpec).cast("integer")) \
                    .orderBy(col("Trend_Rank").asc()) \
                    .withColumn("Process_DataTime", current_timestamp()) \
                    .filter(col("Trend_Rank") <= self.trenOutputNumber)

                trendDF.show(truncate=False)

            batchDF.unpersist()
