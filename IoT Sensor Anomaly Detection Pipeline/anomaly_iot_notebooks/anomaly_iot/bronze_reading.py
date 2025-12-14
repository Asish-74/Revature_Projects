# Databricks notebook source
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
import os

# -----------------------------
# Kinesis Config
# -----------------------------
STREAM_NAME = "anomaly-iot"
REGION = "ap-south-1"

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# -----------------------------
# Schema
# -----------------------------
anomaly_schema = StructType([
    StructField("event_time", LongType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("air_quality", DoubleType()),
    StructField("light", DoubleType()),
    StructField("loudness", DoubleType()),
    StructField("device_id", StringType())
])

# -----------------------------
# Read Kinesis Stream
# -----------------------------
raw_stream = (
    spark.readStream
        .format("kinesis")
        .option("streamName", STREAM_NAME)
        .option("region", REGION)
        .option("awsAccessKey", AWS_ACCESS_KEY_ID)
        .option("awsSecretKey", AWS_SECRET_ACCESS_KEY)
        .option("initialPosition", "LATEST")
        .load()
)

# -----------------------------
# Bronze Transformation
# -----------------------------
bronze_df = (
    raw_stream
        .selectExpr("CAST(data AS STRING) AS json")
        .select(from_json(col("json"), anomaly_schema).alias("d"))
        .select("d.*")
)

display(bronze_df)

# -----------------------------
# Write Bronze Delta Table
# -----------------------------
(
    bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/tmp/anomaly_iot/bronze_checkpoint")
        .toTable("bronze_anomaly_iot")
)
