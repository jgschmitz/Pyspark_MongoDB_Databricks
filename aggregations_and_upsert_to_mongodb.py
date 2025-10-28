from pyspark.sql import functions as F
from common import get_spark

spark = get_spark("agg-and-upsert-to-mongo")

silver = "lakehouse.silver.events_clean"

# Example feature rollups per member per day
features_df = (spark.table(silver)
    .groupBy("member_id", "event_date")
    .agg(
        F.count("*").alias("event_cnt"),
        F.sum("amount").alias("amount_sum"),
        F.expr("percentile_approx(amount, 0.5)").alias("amount_p50")
    )
    .withColumn("run_ts", F.current_timestamp()))

# Upsert (by composite key) into MongoDB
# Use a stable key for upserts (e.g., member_id + event_date)
(features_df.write
    .format("mongodb")
    .mode("append")                      # append with upsert semantics
    .option("collection", "member_daily_features")
    .option("replaceDocument", "false")  # important for $setOnInsert / $set behavior
    .option("operationType", "update")   # enables upsert-style writes
    .option("upsertFields", "member_id,event_date")  # connector uses these as filter keys
    .save())
