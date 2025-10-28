#!/usr/bin/env python3
"""
Basic ETL Pipeline Example
==========================

This example demonstrates a simple ETL pipeline using PySpark and Delta Lake,
following the medallion architecture (Bronze -> Silver -> Gold).

Author: PySpark MongoDB Databricks Examples
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *

# Initialize Spark with Delta Lake support
def create_spark_session():
    """Create Spark session with optimized configurations"""
    builder = SparkSession.builder \
        .appName("BasicETLPipeline") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    
    return configure_spark_with_delta_pip(builder).getOrCreate()

# Define data schemas
def get_raw_events_schema():
    """Define schema for raw event data"""
    return StructType([
        StructField("user_id", StringType(), False),
        StructField("event_time", StringType(), False),  # Will convert to timestamp
        StructField("event_type", StringType(), False),
        StructField("page_url", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("properties", MapType(StringType(), StringType()), True)
    ])

def bronze_layer_ingestion(spark, input_path, output_path):
    """
    Bronze Layer: Raw data ingestion with minimal transformations
    """
    print("🥉 Starting Bronze Layer Ingestion...")
    
    # Read raw JSON files with schema
    raw_df = spark.read \
        .schema(get_raw_events_schema()) \
        .option("multiline", "false") \
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
        .format("json") \
        .load(input_path)
    
    # Add metadata columns
    bronze_df = raw_df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("file_name", input_file_name()) \
        .withColumn("ingestion_date", current_date())
    
    # Write to Delta Lake with partitioning
    bronze_df.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("ingestion_date") \
        .option("mergeSchema", "true") \
        .save(output_path)
    
    print(f"✅ Bronze layer completed. Records processed: {bronze_df.count()}")
    return bronze_df

def silver_layer_transformation(spark, bronze_path, silver_path):
    """
    Silver Layer: Data cleansing and validation
    """
    print("🥈 Starting Silver Layer Transformation...")
    
    # Read from bronze layer
    bronze_df = spark.read.format("delta").load(bronze_path)
    
    # Data cleansing and transformations
    silver_df = bronze_df \
        .filter(col("user_id").isNotNull()) \
        .filter(col("event_time").isNotNull()) \
        .withColumn("event_timestamp", 
                   to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("event_date", 
                   to_date(col("event_timestamp"))) \
        .withColumn("hour_of_day", 
                   hour(col("event_timestamp"))) \
        .withColumn("is_mobile", 
                   when(lower(col("user_agent")).contains("mobile"), True)
                   .otherwise(False)) \
        .filter(col("event_timestamp").isNotNull()) \
        .drop("event_time")  # Remove original string column
    
    # Data quality checks
    null_events = silver_df.filter(
        col("user_id").isNull() | 
        col("event_timestamp").isNull() |
        col("event_type").isNull()
    ).count()
    
    if null_events > 0:
        print(f"⚠️ Warning: Found {null_events} records with null critical fields")
    
    # Write to Delta Lake
    silver_df.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("event_date") \
        .save(silver_path)
    
    print(f"✅ Silver layer completed. Clean records: {silver_df.count()}")
    return silver_df

def gold_layer_aggregation(spark, silver_path, gold_path):
    """
    Gold Layer: Business-ready aggregated data
    """
    print("🥇 Starting Gold Layer Aggregation...")
    
    # Read from silver layer
    silver_df = spark.read.format("delta").load(silver_path)
    
    # Daily user engagement metrics
    daily_metrics = silver_df \
        .groupBy("event_date", "event_type") \
        .agg(
            count("*").alias("event_count"),
            countDistinct("user_id").alias("unique_users"),
            countDistinct("session_id").alias("unique_sessions"),
            avg("hour_of_day").alias("avg_hour_of_day"),
            sum(when(col("is_mobile"), 1).otherwise(0)).alias("mobile_events"),
            sum(when(~col("is_mobile"), 1).otherwise(0)).alias("desktop_events")
        ) \
        .withColumn("mobile_percentage", 
                   round((col("mobile_events") / col("event_count")) * 100, 2)) \
        .withColumn("created_at", current_timestamp())
    
    # Write aggregated data
    daily_metrics.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("event_date") \
        .save(f"{gold_path}/daily_user_engagement")
    
    # Hourly engagement patterns
    hourly_patterns = silver_df \
        .groupBy("event_date", "hour_of_day") \
        .agg(
            count("*").alias("total_events"),
            countDistinct("user_id").alias("active_users"),
            countDistinct("session_id").alias("active_sessions")
        ) \
        .withColumn("created_at", current_timestamp())
    
    hourly_patterns.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("event_date") \
        .save(f"{gold_path}/hourly_engagement_patterns")
    
    print(f"✅ Gold layer completed. Daily metrics: {daily_metrics.count()}")
    print(f"✅ Gold layer completed. Hourly patterns: {hourly_patterns.count()}")

def run_etl_pipeline():
    """
    Main ETL pipeline execution
    """
    print("🚀 Starting ETL Pipeline...")
    
    # Initialize Spark
    spark = create_spark_session()
    
    # Define paths (adjust these for your environment)
    raw_data_path = "/mnt/raw/events/"
    bronze_path = "/mnt/delta/bronze/events"
    silver_path = "/mnt/delta/silver/events"
    gold_path = "/mnt/delta/gold"
    
    try:
        # Execute ETL layers
        bronze_df = bronze_layer_ingestion(spark, raw_data_path, bronze_path)
        silver_df = silver_layer_transformation(spark, bronze_path, silver_path)
        gold_layer_aggregation(spark, silver_path, gold_path)
        
        print("🎉 ETL Pipeline completed successfully!")
        
        # Display sample results
        print("\n📊 Sample Gold Layer Results:")
        daily_metrics = spark.read.format("delta").load(f"{gold_path}/daily_user_engagement")
        daily_metrics.orderBy(desc("event_date")).limit(10).show()
        
    except Exception as e:
        print(f"❌ Pipeline failed: {str(e)}")
        raise
    finally:
        spark.stop()

def optimize_delta_tables():
    """
    Optimize Delta tables for better performance
    """
    spark = create_spark_session()
    
    tables = [
        "/mnt/delta/bronze/events",
        "/mnt/delta/silver/events", 
        "/mnt/delta/gold/daily_user_engagement",
        "/mnt/delta/gold/hourly_engagement_patterns"
    ]
    
    for table_path in tables:
        print(f"🔧 Optimizing table: {table_path}")
        # Run OPTIMIZE command
        spark.sql(f"OPTIMIZE delta.`{table_path}`")
        
        # Run VACUUM to remove old files (use with caution in production)
        # spark.sql(f"VACUUM delta.`{table_path}` RETAIN 168 HOURS")
    
    spark.stop()
    print("✅ All tables optimized!")

if __name__ == "__main__":
    # Run the main ETL pipeline
    run_etl_pipeline()
    
    # Uncomment to run optimization (typically scheduled separately)
    # optimize_delta_tables()