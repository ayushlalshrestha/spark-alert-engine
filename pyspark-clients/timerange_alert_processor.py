"""
Simple stream processor program to fire alerts if more than n logs with
 the same username is detected within a 10-minute window.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType

THRESHOLD = 2
WATERMARK_TIMERANGE = "10 minutes"
SLIDE_DURATION = "1 minutes"

# Create Spark session
spark = SparkSession.builder.appName("KafkaSparkProcessor").getOrCreate()

# Define the schema of the JSON messages
schema = StructType(
    [
        StructField("timestamp", LongType(), True),
        StructField("username", StringType(), True),
    ]
)

# Define Kafka parameters
kafka_params = {
    "kafka.bootstrap.servers": "kafka:9092",
    "subscribe": "logs",
    "startingOffsets": "earliest",
    # "group.id": "Last-10-minutes-alert-processor"
}


# Read from Kafka topic as a DataFrame
df = spark.readStream.format("kafka").options(**kafka_params).load()

# Convert the value column to string and parse the JSON
df = df.selectExpr("CAST(value AS STRING)")
df = df.withColumn("json", from_json(col("value"), schema)).select("json.*")

# Convert the Unix timestamp to Spark timestamp
df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

# Apply windowing and count
windowed_counts = (
    df
    .withWatermark("timestamp", WATERMARK_TIMERANGE)
    .groupBy(
        # window(col("timestamp"), WATERMARK_TIMERANGE, SLIDE_DURATION), col("username")
        window(col("timestamp").cast("timestamp"), WATERMARK_TIMERANGE), col("username")
        # window(col("timestamp").cast("timestamp"), "10 minutes", "5 minutes")
    )
    .count()
)

# Filter for counts greater than THRESHOLD
alerts = windowed_counts.filter(col("count") > THRESHOLD)


# Define the alert action
def alert_action(batch_df, batch_id):
    batch_df.show()
    for row in batch_df.collect():
        print(
            f"Alert fired; user={row.username}, window=\"{row.window.start} to {row.window.end}\", count={row['count']}, threshold={THRESHOLD}, timerange={WATERMARK_TIMERANGE}"
        )


# Write to console (or you can define your own alert action here)
query = alerts.writeStream.outputMode("update").foreachBatch(alert_action).start()

query.awaitTermination()
