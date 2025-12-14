# Databricks notebook source
spark.sql("""
INSERT INTO anomaly_iot.default.gold_device_stats
VALUES (
    'device_100',
    current_timestamp(),
    current_timestamp(),
    20,
    90, 80, 120, 10, 50, 60, 643, 200,
    6.2,
    true,
    'ANOMALY'
)
""")


# COMMAND ----------

spark.sql("""
DELETE FROM anomaly_iot.default.gold_device_stats
WHERE device_id = 'device_100'
""")


# COMMAND ----------

display(spark.table("anomaly_iot.default.gold_device_stats").filter("is_anomaly = true"))


# COMMAND ----------

display(
    spark.table("anomaly_iot.default.gold_device_stats")
          .filter("is_anomaly = true")
          .orderBy("window_start")
)
