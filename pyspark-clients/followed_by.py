from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, when
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Initialize Spark Session
spark = SparkSession.builder.appName("LoginAlertApp").getOrCreate()

# Define schema for incoming logs
schema = StructType(
    [
        StructField("username", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("login_action", StringType()),
    ]
)

# Read from Kafka stream
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "logs")
    .load()
)

# Parse JSON data
parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Create a window and count failed and successful logins
windowed_counts = (
    parsed_df.withWatermark("timestamp", "1 minute")
    .groupBy(window("timestamp", "1 minute"), "username")
    .agg(
        count(when(col("login_action") == "failed", 1)).alias("failed_count"),
        count(when(col("login_action") == "success", 1)).alias("success_count"),
    )
)

# Define alert condition
alert_condition = (col("failed_count") >= 5) & (col("success_count") > 0)

# Filter alerts
alerts = windowed_counts.filter(alert_condition)

# Output alerts
query = alerts.writeStream.outputMode("update").format("console").start()

query.awaitTermination()
