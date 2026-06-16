# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import sum as _sum, count, col
from pyspark.sql.window import Window
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Define the schema

# COMMAND ----------

ride_schema = StructType([
    StructField("ride_id", StringType(), False),
    StructField("event_time", TimestampType(), False),
    StructField("city", StringType(), False),
    StructField("ride_type", StringType(), False),
    StructField("fare", DoubleType(), False),
    StructField("distance_km", DoubleType(), False)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - ReadStream using Auto Loader

# COMMAND ----------

# MAGIC %md
# MAGIC 👉 **.schema(ride_schema)**
# MAGIC Used for:
# MAGIC - Initial schema definition
# MAGIC - Data type enforcement
# MAGIC - Avoid bad data inference
# MAGIC - Production stability
# MAGIC
# MAGIC 👉 **cloudFiles.schemaLocation**
# MAGIC Used for:
# MAGIC - Storing inferred schema state
# MAGIC - Handling schema evolution
# MAGIC - Remembering schema across runs
# MAGIC
# MAGIC **_Auto Loader (cloudFiles)_** is Databricks' optimized file streaming source — it tracks which files have already been processed so you never double-count, giving you exactly-once ingestion.

# COMMAND ----------

# DBTITLE 1,Auto Loader
raw_stream = (
    spark.readStream
        .format("cloudFiles")                   
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation",
                "/Volumes/streaming_demo/ride_booking/checkpoints/schema")
        .schema(ride_schema)
        .load("/Volumes/streaming_demo/ride_booking/raw_rides/")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 — Transform with Windowed Aggregation

# COMMAND ----------



aggregated = (
    raw_stream
    .withWatermark("event_time", "2 minutes")
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("city")
    )
    .agg(
        _sum("fare").alias("total_fare"),
        count("ride_id").alias("ride_count")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("city"),
        col("total_fare"),
        col("ride_count")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6 — Write Stream to Delta Table

# COMMAND ----------

# MAGIC %md
# MAGIC **NOTE: Limitiations in Free Edition to implement writeStream**
# MAGIC 1. In serverless edition Databricks doesn't allow us to run by default trigger options hence we need to explicitly specify trigger option **(.trigger(availableNow=True))** during writeStream.
# MAGIC 2. It also doesn't allow us to use .outputMode("update") instead we need to use **.outputMode("append")**

# COMMAND ----------

checkpoint_path = "/Volumes/streaming_demo/ride_booking/checkpoints/agg_query"

query = (
    aggregated.writeStream
        .format("delta")
        .trigger(availableNow=True)
        .outputMode("append")      
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .toTable("streaming_demo.ride_booking.ride_booking_analytics") 
)

print("✅ Streaming query started:", query.id)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7 — Observe & Query Results

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT city,
# MAGIC        round(total_fare,2) as total_fare,
# MAGIC        ride_count
# MAGIC FROM streaming_demo.ride_booking.ride_booking_analytics
# MAGIC ORDER BY total_fare DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM streaming_demo.ride_booking.ride_booking_analytics;

# COMMAND ----------

