#!/usr/bin/env python3
"""
MongoDB Integration with PySpark
=================================

This example demonstrates best practices for reading from and writing to MongoDB
using PySpark, including optimization techniques and error handling.

Author: PySpark MongoDB Databricks Examples
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

def create_spark_session():
    """Create Spark session with MongoDB connector"""
    return SparkSession.builder \
        .appName("MongoDBIntegration") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

class MongoDBManager:
    """
    Manager class for MongoDB operations with PySpark
    """
    
    def __init__(self, connection_uri="mongodb://localhost:27017"):
        self.spark = create_spark_session()
        self.connection_uri = connection_uri
        
    def read_from_mongodb(self, database, collection, pipeline=None, partitioner="MongoDefaultPartitioner"):
        """
        Read data from MongoDB with optimization
        
        Args:
            database (str): MongoDB database name
            collection (str): MongoDB collection name
            pipeline (list): MongoDB aggregation pipeline (optional)
            partitioner (str): Partitioning strategy for better parallelism
        """
        print(f"📖 Reading from {database}.{collection}...")
        
        reader = self.spark.read \
            .format("mongodb") \
            .option("spark.mongodb.read.connection.uri", self.connection_uri) \
            .option("spark.mongodb.read.database", database) \
            .option("spark.mongodb.read.collection", collection) \
            .option("spark.mongodb.read.partitioner", partitioner)
        
        # Add aggregation pipeline if provided
        if pipeline:
            pipeline_json = json.dumps(pipeline)
            reader = reader.option("pipeline", pipeline_json)
            
        # Configure partitioning for better performance
        if partitioner == "MongoShardedPartitioner":
            reader = reader.option("spark.mongodb.read.partitionerOptions.shardKey", "_id")
        elif partitioner == "MongoSamplePartitioner":
            reader = reader.option("spark.mongodb.read.partitionerOptions.partitionSizeMB", "64")
            
        df = reader.load()
        print(f"✅ Successfully read {df.count()} records")
        return df
    
    def write_to_mongodb(self, df, database, collection, mode="append", 
                        upsert=False, upsert_keys=None, batch_size=1000):
        """
        Write DataFrame to MongoDB with optimization
        
        Args:
            df: PySpark DataFrame to write
            database (str): Target MongoDB database
            collection (str): Target MongoDB collection
            mode (str): Write mode - 'append', 'overwrite', 'ignore'
            upsert (bool): Whether to perform upsert operations
            upsert_keys (list): Keys to use for upsert matching
            batch_size (int): Batch size for writes
        """
        print(f"💾 Writing to {database}.{collection}...")
        
        writer = df.write \
            .format("mongodb") \
            .option("spark.mongodb.write.connection.uri", self.connection_uri) \
            .option("spark.mongodb.write.database", database) \
            .option("spark.mongodb.write.collection", collection) \
            .option("maxBatchSize", str(batch_size)) \
            .mode(mode)
        
        # Configure upsert if requested
        if upsert and upsert_keys:
            writer = writer.option("replaceDocument", "true") \
                          .option("idFieldList", ",".join(upsert_keys))
        else:
            writer = writer.option("replaceDocument", "false")
            
        # Add write concern for data consistency
        writer = writer.option("spark.mongodb.write.writeConcern.w", "majority")
        
        try:
            writer.save()
            print(f"✅ Successfully wrote {df.count()} records")
        except Exception as e:
            print(f"❌ Write failed: {str(e)}")
            raise
    
    def close(self):
        """Close Spark session"""
        self.spark.stop()

def example_read_with_projections():
    """
    Example: Reading with projections to reduce data transfer
    """
    mongo = MongoDBManager()
    
    # Read only specific fields to reduce data transfer
    pipeline = [
        {"$match": {"status": "active", "created_date": {"$gte": "2023-01-01"}}},
        {"$project": {
            "user_id": 1,
            "email": 1,
            "created_date": 1,
            "subscription_type": 1,
            "_id": 0  # Exclude MongoDB ObjectId
        }}
    ]
    
    users_df = mongo.read_from_mongodb(
        database="ecommerce",
        collection="users", 
        pipeline=pipeline,
        partitioner="MongoSamplePartitioner"
    )
    
    # Show sample data
    print("📋 Sample user data:")
    users_df.show(5, truncate=False)
    
    mongo.close()
    return users_df

def example_aggregated_read():
    """
    Example: Reading pre-aggregated data from MongoDB
    """
    mongo = MongoDBManager()
    
    # Complex aggregation pipeline in MongoDB
    pipeline = [
        {"$match": {"order_date": {"$gte": "2023-01-01"}}},
        {"$group": {
            "_id": {
                "customer_id": "$customer_id",
                "month": {"$month": "$order_date"}
            },
            "total_orders": {"$sum": 1},
            "total_amount": {"$sum": "$amount"},
            "avg_order_value": {"$avg": "$amount"}
        }},
        {"$project": {
            "customer_id": "$_id.customer_id",
            "month": "$_id.month", 
            "total_orders": 1,
            "total_amount": 1,
            "avg_order_value": {"$round": ["$avg_order_value", 2]},
            "_id": 0
        }},
        {"$sort": {"customer_id": 1, "month": 1}}
    ]
    
    monthly_summary = mongo.read_from_mongodb(
        database="ecommerce",
        collection="orders",
        pipeline=pipeline
    )
    
    print("📊 Monthly customer summary:")
    monthly_summary.show(10)
    
    mongo.close()
    return monthly_summary

def example_delta_to_mongodb_pattern():
    """
    Example: Delta Lake → MongoDB integration pattern
    This is the recommended approach for large datasets
    """
    spark = create_spark_session()
    mongo = MongoDBManager()
    
    # Step 1: Process data in Delta Lake (fast, ACID transactions)
    print("🔄 Step 1: Processing data in Delta Lake...")
    
    # Read from Delta Lake
    delta_df = spark.read.format("delta").load("/mnt/delta/processed_analytics")
    
    # Perform complex aggregations in Spark (much faster than MongoDB)
    customer_insights = delta_df \
        .groupBy("customer_id") \
        .agg(
            count("order_id").alias("lifetime_orders"),
            sum("amount").alias("lifetime_value"),
            avg("amount").alias("avg_order_value"),
            max("order_date").alias("last_order_date"),
            min("order_date").alias("first_order_date"),
            countDistinct("product_category").alias("categories_purchased")
        ) \
        .withColumn("customer_tier", 
                   when(col("lifetime_value") > 10000, "Platinum")
                   .when(col("lifetime_value") > 5000, "Gold")
                   .when(col("lifetime_value") > 1000, "Silver")
                   .otherwise("Bronze")) \
        .withColumn("updated_at", current_timestamp())
    
    # Step 2: Write curated results to MongoDB for applications
    print("🔄 Step 2: Writing curated results to MongoDB...")
    
    mongo.write_to_mongodb(
        df=customer_insights,
        database="analytics",
        collection="customer_insights",
        mode="overwrite",  # Or use upsert for incremental updates
        upsert=True,
        upsert_keys=["customer_id"],
        batch_size=500  # Smaller batches for upserts
    )
    
    spark.stop()
    mongo.close()

def example_streaming_to_mongodb():
    """
    Example: Streaming data to MongoDB
    """
    spark = create_spark_session()
    
    # Read streaming data from Kafka or other source
    streaming_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "user_events") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse Kafka messages
    parsed_df = streaming_df.select(
        col("timestamp"),
        from_json(col("value").cast("string"), 
                 StructType([
                     StructField("user_id", StringType()),
                     StructField("event_type", StringType()),
                     StructField("properties", MapType(StringType(), StringType()))
                 ])).alias("data")
    ).select("timestamp", "data.*")
    
    # Write stream to MongoDB
    def write_to_mongodb_batch(batch_df, batch_id):
        """Function to write each batch to MongoDB"""
        if batch_df.count() > 0:
            mongo = MongoDBManager()
            mongo.write_to_mongodb(
                df=batch_df,
                database="realtime",
                collection="user_events",
                mode="append",
                batch_size=1000
            )
            mongo.close()
    
    # Start streaming query
    query = parsed_df.writeStream \
        .foreachBatch(write_to_mongodb_batch) \
        .option("checkpointLocation", "/tmp/mongodb_streaming_checkpoint") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    print("🌊 Streaming to MongoDB started. Press Ctrl+C to stop.")
    query.awaitTermination()

def example_error_handling_and_monitoring():
    """
    Example: Error handling and monitoring for MongoDB operations
    """
    mongo = MongoDBManager()
    
    try:
        # Read with error handling
        df = mongo.read_from_mongodb("ecommerce", "orders")
        
        # Add data quality checks
        total_records = df.count()
        null_customer_ids = df.filter(col("customer_id").isNull()).count()
        invalid_amounts = df.filter(col("amount") <= 0).count()
        
        print(f"📈 Data Quality Report:")
        print(f"   Total records: {total_records}")
        print(f"   Null customer IDs: {null_customer_ids} ({(null_customer_ids/total_records)*100:.2f}%)")
        print(f"   Invalid amounts: {invalid_amounts} ({(invalid_amounts/total_records)*100:.2f}%)")
        
        # Log metrics (you could send these to MLflow, CloudWatch, etc.)
        if null_customer_ids > total_records * 0.05:  # More than 5% nulls
            print("⚠️ WARNING: High null rate in customer_id field!")
            
        if invalid_amounts > 0:
            print("⚠️ WARNING: Found records with invalid amounts!")
            
    except Exception as e:
        print(f"❌ MongoDB operation failed: {str(e)}")
        # In production, you might want to:
        # - Send alert to monitoring system
        # - Log to central logging system
        # - Implement retry logic
        raise
    finally:
        mongo.close()

def performance_comparison():
    """
    Example: Performance comparison between different approaches
    """
    import time
    
    mongo = MongoDBManager()
    
    print("🏃‍♂️ Performance Comparison:")
    
    # Approach 1: Read all data then filter in Spark
    start_time = time.time()
    all_data = mongo.read_from_mongodb("ecommerce", "orders")
    filtered_spark = all_data.filter(col("amount") > 100)
    result1_count = filtered_spark.count()
    time1 = time.time() - start_time
    
    print(f"Approach 1 (Filter in Spark): {result1_count} records in {time1:.2f}s")
    
    # Approach 2: Filter in MongoDB then read
    start_time = time.time()
    pipeline = [{"$match": {"amount": {"$gt": 100}}}]
    filtered_mongo = mongo.read_from_mongodb("ecommerce", "orders", pipeline=pipeline)
    result2_count = filtered_mongo.count()
    time2 = time.time() - start_time
    
    print(f"Approach 2 (Filter in MongoDB): {result2_count} records in {time2:.2f}s")
    print(f"Performance improvement: {((time1-time2)/time1)*100:.1f}%")
    
    mongo.close()

if __name__ == "__main__":
    print("🚀 MongoDB Integration Examples")
    
    # Run examples
    try:
        print("\n" + "="*50)
        print("Example 1: Reading with Projections")
        example_read_with_projections()
        
        print("\n" + "="*50)
        print("Example 2: Aggregated Read")
        example_aggregated_read()
        
        print("\n" + "="*50)
        print("Example 3: Delta to MongoDB Pattern")
        # example_delta_to_mongodb_pattern()  # Uncomment when Delta path exists
        
        print("\n" + "="*50)
        print("Example 4: Error Handling and Monitoring")
        # example_error_handling_and_monitoring()  # Uncomment when MongoDB is available
        
        print("\n" + "="*50)
        print("Example 5: Performance Comparison")
        # performance_comparison()  # Uncomment when MongoDB is available
        
        print("\n🎉 All examples completed successfully!")
        
    except Exception as e:
        print(f"❌ Example failed: {str(e)}")
        print("💡 Note: Some examples require MongoDB to be running and populated with data")