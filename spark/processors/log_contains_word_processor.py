from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize SparkSession
spark = SparkSession.builder.appName("SimpleKafkaAlertProcessor").getOrCreate()

# Define Kafka parameters
kafka_params = {
    "kafka.bootstrap.servers": "kafka:9092",
    "subscribe": "test",
}

# Read from Kafka topic as a DataFrame
df = spark.readStream.format("kafka").options(**kafka_params).load()

# Convert the value column to string and filter for logs containing the word "threat"
filtered_df = df.selectExpr("CAST(value AS STRING)").filter(
    col("value").contains("threat")
)


# Define a function to print "alert fired" when a log containing "threat" is detected
def process_alerts(batch_df, epoch_id):
    if not batch_df.isEmpty():
        print("Alert fired")


# Start the query to process the alerts
query = filtered_df.writeStream.foreachBatch(process_alerts).start()

# Wait for the streaming query to finish
query.awaitTermination()

# Stop SparkSession
spark.stop()
