# Databricks notebook source
# MAGIC %md
# MAGIC ## SILVER LAYER OPERATIONS

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Read Bronze as a Stream

# COMMAND ----------

from pyspark.sql.functions import (
    col, trim, to_date, hour
)

bronze_stream_df = (
    spark.readStream
         .table("bronze_anomaly_iot")
)

# display(bronze_stream_df.limit(5))


# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Basic Cleaning, Rename & Timestamp Conversion
# MAGIC Operations:
# MAGIC - Rename event_time → event_time_ms
# MAGIC - Convert UNIX ms → timestamp
# MAGIC - Trim device_id
# MAGIC - Drop rows with invalid device_id or timestamp

# COMMAND ----------

silver_df = (
    bronze_stream_df
        .withColumnRenamed("event_time", "event_time_ms")
        .withColumn("event_timestamp", (col("event_time_ms") / 1000).cast("timestamp"))
        .withColumn("device_id", trim(col("device_id")))
        .filter(col("device_id").isNotNull() & (col("device_id") != ""))
        .filter(col("event_timestamp").isNotNull())
)

silver_df.printSchema()
display(silver_df.limit(5))


# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Handle Out-of-Range Sensor Values
# MAGIC Rules enforced:
# MAGIC
# MAGIC - Temperature: -20°C to 80°C
# MAGIC - Humidity: 0% to 100%
# MAGIC - Air quality ≥ 0
# MAGIC - Light ≥ 0
# MAGIC - Loudness ≥ 0

# COMMAND ----------

from pyspark.sql.functions import col

silver_df = (
    silver_df
        .filter((col("temperature") >= -20) & (col("temperature") <= 80))
        .filter((col("humidity") >= 0) & (col("humidity") <= 100))
        .filter(col("air_quality") >= 0)
        .filter(col("light") >= 0)
        .filter(col("loudness") >= 0)
)

display(silver_df.limit(5))


# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Add Derived Time Columns
# MAGIC Added columns:
# MAGIC
# MAGIC - event_date
# MAGIC - event_hour

# COMMAND ----------

silver_df = (
    silver_df
        .withColumn("event_date", to_date(col("event_timestamp")))
        .withColumn("event_hour", hour(col("event_timestamp")))
)

display(silver_df.limit(5))


# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Select Final Column Order
# MAGIC (Clean and production-friendly layout)

# COMMAND ----------

silver_df = silver_df.select(
    "device_id",
    "event_time_ms",
    "event_timestamp",
    "event_date",
    "event_hour",
    "temperature",
    "humidity",
    "air_quality",
    "light",
    "loudness"
)

display(silver_df.limit(5))


# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Write Silver Table as Streaming Delta

# COMMAND ----------

(
    silver_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/tmp/anomaly_iot/silver_checkpoint")
        .toTable("silver_anomaly_iot")      # New Silver table
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Verify Silver Table Output

# COMMAND ----------

display(spark.table("silver_anomaly_iot").limit(20))