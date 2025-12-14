import os
from pyspark.sql.functions import col, current_timestamp
import requests

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

gold_anomalies = (
    spark.readStream
        .table("anomaly_iot.default.gold_device_stats")
        .filter(col("is_anomaly") == True)
)

def post_to_slack(batch_df, batch_id):
    rows = batch_df.collect()
    for r in rows:
        message = {
            "text": (
                "🚨 *IoT Sensor Anomaly Detected!*\n"
                f"*Device:* {r.device_id}\n"
                f"*Window:* {r.window_start} → {r.window_end}\n"
                f"*Avg Temp:* {round(r.avg_temperature, 2)} °C\n"
                f"*Avg Humidity:* {round(r.avg_humidity, 2)}%\n"
                f"*Z-Score:* {round(r.temperature_z_score, 2)}\n"
                f"*Status:* {r.status}"
            )
        }
        try:
            requests.post(SLACK_WEBHOOK_URL, json=message, timeout=5)
        except:
            pass

(
    gold_anomalies
        .withColumn("alert_time", current_timestamp())
        .writeStream
        .foreachBatch(post_to_slack)
        .outputMode("append")
        .option("checkpointLocation", "/tmp/anomaly_iot/slack_checkpoint")
        .trigger(processingTime="10 seconds")
        .start()
)
