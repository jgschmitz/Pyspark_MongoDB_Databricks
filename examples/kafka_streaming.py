#!/usr/bin/env python3
"""
Kafka Streaming with PySpark
=============================

This example demonstrates real-time data processing using PySpark Structured Streaming
with Kafka, including exactly-once processing, watermarks, and monitoring.

Author: PySpark MongoDB Databricks Examples
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.streaming import Trigger
import json

def create_spark_session():
    """Create Spark session optimized for streaming"""
    return SparkSession.builder \
        .appName("KafkaStreaming") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.streaming.metricsEnabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

def get_event_schema():
    """Define schema for incoming event data"""
    return StructType([
        StructField("user_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("event_time", StringType(), False),
        StructField("session_id", StringType(), True),
        StructField("page_url", StringType(), True),
        StructField("referrer", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("properties", MapType(StringType(), StringType()), True)
    ])

class KafkaStreamProcessor:
    """
    Kafka Stream Processor with best practices
    """
    
    def __init__(self, kafka_servers="localhost:9092"):
        self.spark = create_spark_session()
        self.kafka_servers = kafka_servers
        self.event_schema = get_event_schema()
        
    def read_from_kafka(self, topic, starting_offsets="latest"):
        """
        Read streaming data from Kafka with proper configuration
        """
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", starting_offsets) \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", "10000") \
            .load()
    
    def parse_kafka_messages(self, kafka_df):
        """
        Parse Kafka messages and extract event data
        """
        return kafka_df.select(
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("key").cast("string").alias("message_key"),
            from_json(
                col("value").cast("string"), 
                self.event_schema
            ).alias("event_data")
        ).select(
            "partition",
            "offset", 
            "kafka_timestamp",
            "message_key",
            "event_data.*"
        ).withColumn(
            "event_timestamp",
            to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        ).filter(
            col("event_timestamp").isNotNull()
        )
    
    def real_time_analytics_pipeline(self):
        """
        Real-time analytics pipeline with windowed aggregations
        """
        print("🌊 Starting real-time analytics pipeline...")
        
        # Read from Kafka
        kafka_df = self.read_from_kafka("user_events")
        
        # Parse messages
        events_df = self.parse_kafka_messages(kafka_df)
        
        # Add watermark for handling late data
        events_with_watermark = events_df \
            .withWatermark("event_timestamp", "10 minutes")
        
        # Real-time metrics - 5-minute windows
        windowed_metrics = events_with_watermark \
            .groupBy(
                window(col("event_timestamp"), "5 minutes"),
                col("event_type")
            ) \
            .agg(
                count("*").alias("event_count"),
                countDistinct("user_id").alias("unique_users"),
                countDistinct("session_id").alias("unique_sessions"),
                approx_count_distinct("ip_address").alias("unique_ips")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("event_type"),
                col("event_count"),
                col("unique_users"),
                col("unique_sessions"),
                col("unique_ips"),
                current_timestamp().alias("processed_at")
            )
        
        # Write to Delta Lake for exactly-once processing
        query = windowed_metrics.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", "/tmp/streaming_checkpoint/analytics") \
            .option("path", "/tmp/delta/real_time_metrics") \
            .trigger(Trigger.ProcessingTime("30 seconds")) \
            .start()
        
        return query
    
    def session_analytics_pipeline(self):
        """
        Session-based analytics with state management
        """
        print("🔄 Starting session analytics pipeline...")
        
        kafka_df = self.read_from_kafka("user_events")
        events_df = self.parse_kafka_messages(kafka_df)
        
        # Session metrics with watermark
        session_events = events_df \
            .withWatermark("event_timestamp", "30 minutes") \
            .groupBy(
                col("session_id"),
                col("user_id"),
                window(col("event_timestamp"), "30 minutes")
            ) \
            .agg(
                count("*").alias("page_views"),
                min("event_timestamp").alias("session_start"),
                max("event_timestamp").alias("session_end"),
                collect_set("page_url").alias("pages_visited"),
                first("user_agent").alias("user_agent"),
                first("referrer").alias("referrer")
            ) \
            .select(
                col("session_id"),
                col("user_id"),
                col("page_views"),
                col("session_start"),
                col("session_end"),
                ((unix_timestamp("session_end") - unix_timestamp("session_start")) / 60)
                    .alias("session_duration_minutes"),
                size("pages_visited").alias("unique_pages"),
                col("user_agent"),
                col("referrer"),
                current_timestamp().alias("processed_at")
            )
        
        # Write session data
        query = session_events.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", "/tmp/streaming_checkpoint/sessions") \
            .option("path", "/tmp/delta/user_sessions") \
            .trigger(Trigger.ProcessingTime("1 minute")) \
            .start()
        
        return query
    
    def fraud_detection_pipeline(self):
        """
        Real-time fraud detection pipeline
        """
        print("🚨 Starting fraud detection pipeline...")
        
        kafka_df = self.read_from_kafka("user_events") 
        events_df = self.parse_kafka_messages(kafka_df)
        
        # Detect suspicious patterns
        suspicious_events = events_df \
            .withWatermark("event_timestamp", "5 minutes") \
            .groupBy(
                col("ip_address"),
                window(col("event_timestamp"), "5 minutes")
            ) \
            .agg(
                count("*").alias("event_count"),
                countDistinct("user_id").alias("unique_users"),
                countDistinct("session_id").alias("unique_sessions")
            ) \
            .filter(
                (col("event_count") > 100) |  # High volume from single IP
                (col("unique_users") > 10)    # Multiple users from single IP
            ) \
            .select(
                col("ip_address"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("event_count"),
                col("unique_users"),
                col("unique_sessions"),
                when(col("event_count") > 100, "HIGH_VOLUME")
                .when(col("unique_users") > 10, "MULTIPLE_USERS")
                .otherwise("SUSPICIOUS").alias("alert_type"),
                current_timestamp().alias("alert_time")
            )
        
        # Write alerts
        query = suspicious_events.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", "/tmp/streaming_checkpoint/fraud") \
            .option("path", "/tmp/delta/fraud_alerts") \
            .trigger(Trigger.ProcessingTime("10 seconds")) \
            .start()
        
        return query
    
    def streaming_etl_to_mongodb(self):
        """
        Stream processing with MongoDB sink
        """
        print("🍃 Starting streaming ETL to MongoDB...")
        
        kafka_df = self.read_from_kafka("user_events")
        events_df = self.parse_kafka_messages(kafka_df)
        
        # Enrich and transform data
        enriched_df = events_df \
            .withColumn("event_date", to_date("event_timestamp")) \
            .withColumn("hour_of_day", hour("event_timestamp")) \
            .withColumn("is_mobile", 
                       when(lower(col("user_agent")).contains("mobile"), True)
                       .otherwise(False)) \
            .withColumn("domain", 
                       regexp_extract(col("page_url"), "https?://([^/]+)", 1))
        
        # Write to MongoDB using foreachBatch
        def write_to_mongodb(batch_df, batch_id):
            """Write each batch to MongoDB"""
            if batch_df.count() > 0:
                batch_df.write \
                    .format("mongodb") \
                    .option("spark.mongodb.write.connection.uri", 
                           "mongodb://localhost:27017") \
                    .option("spark.mongodb.write.database", "realtime") \
                    .option("spark.mongodb.write.collection", "user_events") \
                    .option("maxBatchSize", "1000") \
                    .mode("append") \
                    .save()
                print(f"✅ Batch {batch_id} written to MongoDB: {batch_df.count()} records")
        
        query = enriched_df.writeStream \
            .foreachBatch(write_to_mongodb) \
            .option("checkpointLocation", "/tmp/streaming_checkpoint/mongodb") \
            .trigger(Trigger.ProcessingTime("30 seconds")) \
            .start()
        
        return query
    
    def monitoring_and_alerts(self):
        """
        Monitoring pipeline for stream health
        """
        print("📊 Starting monitoring pipeline...")
        
        kafka_df = self.read_from_kafka("user_events")
        events_df = self.parse_kafka_messages(kafka_df)
        
        # Monitor data quality and volume
        monitoring_metrics = events_df \
            .withWatermark("event_timestamp", "2 minutes") \
            .groupBy(window(col("event_timestamp"), "1 minute")) \
            .agg(
                count("*").alias("total_events"),
                sum(when(col("user_id").isNull(), 1).otherwise(0)).alias("null_user_ids"),
                sum(when(col("event_type").isNull(), 1).otherwise(0)).alias("null_event_types"),
                countDistinct("partition").alias("active_partitions"),
                avg("offset").alias("avg_offset")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("total_events"),
                col("null_user_ids"),
                col("null_event_types"),
                (col("null_user_ids").cast("double") / col("total_events") * 100)
                    .alias("null_user_id_percentage"),
                col("active_partitions"),
                col("avg_offset"),
                current_timestamp().alias("processed_at")
            )
        
        # Console output for monitoring
        query = monitoring_metrics.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .trigger(Trigger.ProcessingTime("30 seconds")) \
            .start()
        
        return query
    
    def stop_all_streams(self):
        """Stop all active streams"""
        active_streams = self.spark.streams.active
        for stream in active_streams:
            stream.stop()
        self.spark.stop()

def run_multiple_pipelines():
    """
    Run multiple streaming pipelines concurrently
    """
    processor = KafkaStreamProcessor()
    
    try:
        # Start multiple pipelines
        analytics_query = processor.real_time_analytics_pipeline()
        session_query = processor.session_analytics_pipeline() 
        fraud_query = processor.fraud_detection_pipeline()
        monitoring_query = processor.monitoring_and_alerts()
        
        print("🚀 All streaming pipelines started!")
        print("📊 Monitor progress in Spark UI: http://localhost:4040")
        print("Press Ctrl+C to stop all streams...")
        
        # Wait for termination
        analytics_query.awaitTermination()
        
    except KeyboardInterrupt:
        print("\n⏹️ Stopping all streams...")
        processor.stop_all_streams()
        print("✅ All streams stopped successfully!")
    except Exception as e:
        print(f"❌ Streaming failed: {str(e)}")
        processor.stop_all_streams()
        raise

def run_simple_streaming_example():
    """
    Simple streaming example for getting started
    """
    processor = KafkaStreamProcessor()
    
    try:
        print("🌊 Starting simple streaming example...")
        
        # Basic streaming query
        kafka_df = processor.read_from_kafka("user_events", "earliest")
        events_df = processor.parse_kafka_messages(kafka_df)
        
        # Simple console output
        query = events_df.select(
            "user_id", "event_type", "event_timestamp", "page_url"
        ).writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .trigger(Trigger.ProcessingTime("5 seconds")) \
            .start()
        
        print("📺 Events will appear in console...")
        query.awaitTermination()
        
    except KeyboardInterrupt:
        print("\n⏹️ Stopping stream...")
        processor.stop_all_streams()
    except Exception as e:
        print(f"❌ Stream failed: {str(e)}")
        processor.stop_all_streams()

if __name__ == "__main__":
    print("🚀 Kafka Streaming with PySpark")
    print("Choose an option:")
    print("1. Simple streaming example")
    print("2. Multiple pipelines")
    
    choice = input("Enter choice (1 or 2): ").strip()
    
    if choice == "1":
        run_simple_streaming_example()
    elif choice == "2":
        run_multiple_pipelines()
    else:
        print("Invalid choice. Running simple example...")
        run_simple_streaming_example()