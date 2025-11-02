from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import from_json, col
from pyspark.sql import SparkSession

# ---------------------------
# Event Hubs Configuration
# ---------------------------

event_hub_sas_key_name = ""
event_hub_sas_key = ""
event_hub_name = ""
event_hub_namespace = ""

connection_string = (
    f"Endpoint=sb://{event_hub_namespace}.servicebus.windows.net/;"
    f"SharedAccessKeyName={event_hub_sas_key_name};"
    f"SharedAccessKey={event_hub_sas_key};"
    f"EntityPath={event_hub_name}"
)

event_hub_config = {
    "eventhubs.connectionString": connection_string,
    "eventhubs.consumerGroup": "$Default",
    "eventhubs.startingPosition": "@latest"
}

# ---------------------------
# Spark Session Setup
# ---------------------------

spark = SparkSession.builder \
    .appName("EventHub-To-DeltaLake-Streaming") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# ---------------------------
# Stream Read from Event Hubs
# ---------------------------

event_hub_stream = spark.readStream \
    .format("eventhubs") \
    .options(**event_hub_config) \
    .load()

# ---------------------------
# Message Schema
# ---------------------------

schema = StructType([
    StructField("rawValue", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])

parsed_stream = (
    event_hub_stream
    .withColumn("body", col("body").cast("string"))
    .select(from_json(col("body"), schema).alias("data"))
    .select("data.*")
)

# ---------------------------
# Delta Lake Output Path
# ---------------------------

storage_container = ""
storage_account = ""
delta_table_path = ""

delta_output_path = (
    f"abfss://{storage_container}@{storage_account}.dfs.core.windows.net/{delta_table_path}"
)

checkpoint_path = "/mnt/delta/checkpoints/eventhub_to_delta"

# ---------------------------
# Write Stream to Delta Lake
# ---------------------------

query = (
    parsed_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .start(delta_output_path)
)

query.awaitTermination()