# Databricks notebook source
gold_df = spark.table("anomaly_iot.default.gold_device_stats")
display(gold_df)


# COMMAND ----------

# MAGIC %md
# MAGIC 1. Average Temperature Trend

# COMMAND ----------

# DBTITLE 1,Average Temperature Trend
df = gold_df.select("window_start", "avg_temperature")
display(df)
# df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC 2. Anomaly Trend (When anomalies occur)

# COMMAND ----------

# DBTITLE 1,Anomaly Trend (When anomalies occur)
df = gold_df.select("window_start", "is_anomaly")
display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC 3. Compare Min/Max/Avg Temperature per Device

# COMMAND ----------

df = gold_df.select("device_id", "avg_temperature", "min_temperature", "max_temperature")
display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC 4. Distribution of Z-scores
# MAGIC how many windows fall in the z-score

# COMMAND ----------

# DBTITLE 1,Distribution of Z-scores
df = gold_df.select("temperature_z_score")
display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC 5. Temperature vs Humidity Scatter

# COMMAND ----------

df = gold_df.select("avg_temperature", "avg_humidity")
display(df)


# COMMAND ----------

