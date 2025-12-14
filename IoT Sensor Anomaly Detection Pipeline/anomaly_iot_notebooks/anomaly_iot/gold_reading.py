# Databricks notebook source
# MAGIC %md
# MAGIC ## **GOLD LAYER -  Device Stats & Anomaly Detection**
# MAGIC ---------------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Read Silver Layer as a Streaming Source

# COMMAND ----------

from pyspark.sql import functions as F

# If using Unity Catalog, i can do:
# SILVER_TABLE = "iot_catalog.processed.valid_readings"
SILVER_TABLE = "silver_anomaly_iot"   # this is existing  Silver table

silver_stream_df = (
    spark.readStream
         .table(SILVER_TABLE)
)

display(silver_stream_df.limit(5))


# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Build 5-Minute Window Aggregates per Device
# MAGIC -----------------------------------------------
# MAGIC We compute per device + 5-minute window:
# MAGIC - event_count
# MAGIC - avg / min / max temperature
# MAGIC - standard deviation of temperature temperature (for z-score)
# MAGIC - avg humidity, air_quality, light, loudness

# COMMAND ----------

from pyspark.sql.functions import (
    col, window, count, avg, stddev_pop, min, max
)

gold_agg_df = (
    silver_stream_df
        # Watermark so old late data is dropped after 10 minutes
        .withWatermark("event_timestamp", "10 minutes")

        # Group by device + 5-minute tumbling window
        .groupBy(
            col("device_id"),
            window(col("event_timestamp"), "5 minutes", "5 minutes").alias("time_window")
        )

        # Aggregations for metrics
        .agg(
            count("*").alias("event_count"),
            avg("temperature").alias("avg_temperature"),
            stddev_pop("temperature").alias("std_temperature"),
            min("temperature").alias("min_temperature"),
            max("temperature").alias("max_temperature"),
            avg("humidity").alias("avg_humidity"),
            avg("air_quality").alias("avg_air_quality"),
            avg("light").alias("avg_light"),
            avg("loudness").alias("avg_loudness")
        )

        # Flatten window struct into start/end columns
        .withColumn("window_start", col("time_window").start)
        .withColumn("window_end", col("time_window").end)
        .drop("time_window")
)

display(gold_agg_df.limit(5))
gold_agg_df.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Compute Z-Score–Style Anomaly & Status Flag
# MAGIC
# MAGIC We’ll treat a window as anomalous when:
# MAGIC
# MAGIC z = ( max_temperature - avg_temperature ) / std_temperature > 3
# MAGIC
# MAGIC Then:
# MAGIC - is_anomaly = true/false
# MAGIC - status = "ANOMALY" or "NORMAL"

# COMMAND ----------

from pyspark.sql.functions import when, lit

# Temperature z-score at window level
gold_with_anomaly_df = (
    gold_agg_df
        # Avoid divide-by-zero when std_temperature is 0 or null
        .withColumn(
            "temperature_z_score",
            when(
                (col("std_temperature").isNotNull()) & (col("std_temperature") > 0),
                (col("max_temperature") - col("avg_temperature")) / col("std_temperature")
            ).otherwise(lit(0.0))
        )
        # Anomaly flag (|z| > 3)
        .withColumn(
            "is_anomaly",
            (F.abs(col("temperature_z_score")) > lit(3.0))
        )
        # Human-readable status
        .withColumn(
            "status",
            when(col("is_anomaly"), lit("ANOMALY")).otherwise(lit("NORMAL"))
        )
)

display(gold_with_anomaly_df.limit(5))


# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Select Final Gold Schema (device_stats)

# COMMAND ----------

GOLD_TABLE = "gold_device_stats"   # Using Hive Metastore (default)
    
device_stats_df = gold_with_anomaly_df.select(
    "device_id",
    "window_start",
    "window_end",
    "event_count",
    "avg_temperature",
    "min_temperature",
    "max_temperature",
    "std_temperature",
    "avg_humidity",
    "avg_air_quality",
    "avg_light",
    "avg_loudness",
    "temperature_z_score",
    "is_anomaly",
    "status"
)

display(device_stats_df.limit(5))


# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Write Gold Device Stats as Streaming Delta (10-Second Micro-Batches)

# COMMAND ----------

GOLD_TABLE = "anomaly_iot.default.gold_device_stats"

device_stats_query = (
    device_stats_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/tmp/anomaly_iot/gold_device_stats_checkpoint")
        .trigger(processingTime="10 seconds")
        .toTable("anomaly_iot.default.gold_device_stats")
)
display(spark.table("anomaly_iot.default.gold_device_stats"))


# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Create Anomaly Log Stream (iot_catalog.logs.anomaly_log)
# MAGIC We’ll filter only anomalous windows and write them to a separate Delta table.

# COMMAND ----------

ANOMALY_LOG_TABLE = "anomaly_iot.default.anomaly_log"

anomaly_log_df = (
    device_stats_df
        .filter(col("is_anomaly") == True)
        .withColumn("logged_at", F.current_timestamp())
)

anomaly_log_query = (
    anomaly_log_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/tmp/anomaly_iot/anomaly_log_checkpoint")
        .trigger(processingTime="10 seconds")
        .toTable(ANOMALY_LOG_TABLE)
)
display(spark.table("anomaly_iot.default.gold_device_stats"))

