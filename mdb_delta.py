# 01_read_mongodb_to_delta.py
from pyspark.sql.types import *
from pyspark.sql import functions as F
from common import get_spark

spark = get_spark("read-mongo-to-delta")

# Define schema explicitly (avoid inferSchema)
schema = StructType([
    StructField("_id", StringType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("member_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("attributes", MapType(StringType(), StringType()), True)
])

bronze_path = "/mnt/lake/bronze/events"
silver_table = "lakehouse.silver.events_clean"

# Read from MongoDB
raw_df = (spark.read
          .format("mongodb")
          .option("collection", spark.conf.get("spark.mongodb.read.collection"))
          .schema(schema)
          .load())

# Basic cleanup / normalization
clean_df = (raw_df
            .withColumn("event_date", F.to_date("event_time"))
            .withColumn("amount", F.coalesce(F.col("amount"), F.lit(0.0)))
            .filter(F.col("event_type").isNotNull()))

# Write Bronze (append-only, partitioned by date)
(clean_df.write
    .format("delta")
    .mode("append")
    .partitionBy("event_date")
    .save(bronze_path))

# Create/merge into a Silver table (idempotent)
spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.silver.events_clean
USING DELTA
LOCATION '/mnt/lake/bronze/events'
""")
