# PySpark MongoDB Databricks Integration

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.4+-orange.svg)](https://spark.apache.org/)

A comprehensive guide and example collection for building scalable data pipelines using PySpark, MongoDB, and Databricks. This repository provides best practices, performance optimization techniques, and real-world examples for modern data engineering workflows.

## 📋 Table of Contents

- [When to Use PySpark](#when-to-use-pyspark)
- [When NOT to Use PySpark](#when-not-to-use-pyspark)
- [Data Loading & Ingestion](#data-loading--ingestion)
- [Transform & Join Performance](#transform--join-performance)
- [Partitioning & File Management](#partitioning--file-management)
- [Streaming Specifics](#streaming-specifics)
- [MongoDB Integration](#mongodb-integration)
- [Reliability & Governance](#reliability--governance)
- [Cost & Cluster Management](#cost--cluster-management)
- [Quick Reference](#quick-reference)
- [Examples](#examples)

## ✅ When to Use PySpark

### Large/Distributed Data Processing
Use PySpark when your data won't fit on a single machine or you need parallelism:
- **Multi-TB datasets**
- **Wide joins across large tables**
- **Complex window operations at scale**
- **Parallel processing requirements**

```python
# Example: Processing large datasets with PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("LargeDataProcessing") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Processing multi-TB dataset with window functions
df = spark.read.format("delta").load("/mnt/data/large_dataset")

window_spec = Window.partitionBy("customer_id").orderBy("timestamp")
result = df.withColumn("running_total", 
                      sum("amount").over(window_spec))
```

### Real-time Streaming
Perfect for Kafka ingestion and exactly-once processing:
- **Kafka/Auto Loader ingestion**
- **Exactly-once sinks to Delta Lake**
- **Real-time data processing**

### Heavy Transformations
Ideal for complex data transformations:
- **Wide joins**
- **Complex groupBy/aggregations**
- **Window functions at scale**

### Governed Lakehouse Architecture
Essential for data governance:
- **Writing to Delta Lake with schema enforcement**
- **ACID transactions**
- **Time travel and versioning**

## ❌ When NOT to Use PySpark

### Small/Interactive Data
**Use pandas/SQL instead for:**
- **< 1-2 GB datasets**
- **Ad-hoc analysis**
- **Interactive exploration**

```python
# Instead of PySpark for small data:
import pandas as pd
df = pd.read_csv("small_dataset.csv")
result = df.groupby("category").sum()
```

### Low-latency Serving
**Use app services/feature stores for:**
- **Sub-100ms online inference**
- **Real-time API responses**
- **Interactive applications**

### Tight Iterative Loops
**Use scikit-learn for:**
- **Classic ML model training**
- **Iterative algorithms in Python loops**
- **Small-scale experimentation**

*Note: Use Spark MLlib only when data size forces distribution*

## 📥 Data Loading & Ingestion

### Best Practices

**Prefer structured formats:**
```python
# ✅ Good: Use Delta/Parquet
df = spark.read.format("delta").load("/mnt/data/events")

# ❌ Avoid: CSV/JSON for production workloads
df = spark.read.format("csv").load("/mnt/raw/events.csv")
```

**Always define schemas:**
```python
from pyspark.sql.types import *

# Define schema explicitly for stability
my_schema = StructType([
    StructField("user_id", StringType(), False),
    StructField("event_time", TimestampType(), False),
    StructField("event_type", StringType(), False),
    StructField("properties", MapType(StringType(), StringType()), True)
])

df = (spark.read
      .schema(my_schema)
      .format("json")
      .load("/mnt/raw/events/"))
```

### Auto Loader for Incremental Processing

```python
# Auto Loader: Files → Delta (handles new files, schema drift, checkpoints)
(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "json")
 .option("cloudFiles.schemaLocation", "/mnt/schema/events")
 .schema(my_schema)
 .load("/mnt/raw/events/")
 .writeStream
 .format("delta")
 .option("checkpointLocation", "/mnt/checkpoints/events")
 .option("mergeSchema", "true")
 .toTable("bronze.events"))
```

### Kafka Integration

```python
# Kafka streaming with proper configuration
kafka_df = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", "localhost:9092")
           .option("subscribe", "events")
           .option("startingOffsets", "latest")
           .option("failOnDataLoss", "false")  # Only if you accept gaps
           .load())

# Parse Kafka messages
parsed_df = kafka_df.select(
    col("key").cast("string"),
    from_json(col("value").cast("string"), my_schema).alias("data"),
    col("timestamp")
).select("key", "data.*", "timestamp")
```

## ⚡ Transform & Join Performance

### Optimization Strategies

**Push work to the Spark engine:**
```python
# ✅ Good: Use Spark SQL/DataFrame operations
df.filter(col("amount") > 1000) \
  .groupBy("category") \
  .agg(sum("amount").alias("total"))

# ❌ Avoid: Python UDFs when possible
from pyspark.sql.functions import udf
expensive_udf = udf(lambda x: complex_python_logic(x))
```

**Use Pandas UDFs for better performance:**
```python
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType
import pandas as pd

@pandas_udf(returnType=DoubleType())
def vectorized_computation(values: pd.Series) -> pd.Series:
    # Vectorized operations are much faster
    return values.apply(complex_python_logic)

df.withColumn("computed", vectorized_computation(col("input_column")))
```

### Join Optimization

```python
# Broadcast small tables to avoid shuffles
from pyspark.sql.functions import broadcast

large_df = spark.table("sales")
small_lookup = spark.table("product_categories")

# ✅ Broadcast join for small tables (< 10MB)
result = large_df.join(broadcast(small_lookup), "product_id")

# Handle data skew
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
```

### Caching Strategy

```python
# Cache only if reused 2+ times and fits in memory
expensive_df = df.groupBy("customer_id").agg(
    sum("amount").alias("total_spent"),
    count("*").alias("transaction_count")
).cache()

# Use the cached DataFrame multiple times
high_value_customers = expensive_df.filter(col("total_spent") > 10000)
frequent_customers = expensive_df.filter(col("transaction_count") > 50)

# Always unpersist when done
expensive_df.unpersist()
```

## 🗂️ Partitioning & File Management

### Delta Lake Partitioning

```python
# ✅ Partition on low-cardinality columns used for filtering
df.write \
  .format("delta") \
  .partitionBy("event_date", "region") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/mnt/delta/events")

# ❌ Avoid high-cardinality partitions
# .partitionBy("user_id")  # Bad: too many partitions
```

### File Size Optimization

```python
# Avoid small files: target 128-512 MB per file
df.coalesce(10) \
  .write \
  .format("delta") \
  .mode("append") \
  .save("/mnt/delta/optimized_table")

# Use Auto Optimize (Databricks)
spark.sql("""
    ALTER TABLE my_table 
    SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
""")

# Manual optimization
spark.sql("OPTIMIZE my_table ZORDER BY (commonly_filtered_column)")
```

## 🌊 Streaming Specifics

### Trigger Configuration

```python
from pyspark.sql.streaming import Trigger

# Micro-batch processing
stream = df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/mnt/checkpoints/stream") \
    .trigger(Trigger.ProcessingTime("1 minute")) \
    .start("/mnt/delta/stream_output")

# One-time batch processing
stream = df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/mnt/checkpoints/batch") \
    .trigger(Trigger.Once()) \
    .start("/mnt/delta/batch_output")
```

### Watermarks and State Management

```python
# Use watermarks to bound state in windowed operations
windowed_df = df \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("user_id")
    ) \
    .agg(sum("amount").alias("total_amount"))
```

## 🍃 MongoDB Integration

### Reading from MongoDB

```python
# Read with projections and filters for pushdown optimization
mongo_df = spark.read \
    .format("mongodb") \
    .option("spark.mongodb.read.connection.uri", "mongodb://localhost:27017") \
    .option("spark.mongodb.read.database", "analytics") \
    .option("spark.mongodb.read.collection", "events") \
    .option("spark.mongodb.read.partitioner", "MongoShardedPartitioner") \
    .option("spark.mongodb.read.partitionerOptions.shardKey", "user_id") \
    .load()

# Use projections to limit data transfer
projected_df = spark.read \
    .format("mongodb") \
    .option("spark.mongodb.read.connection.uri", "mongodb://localhost:27017") \
    .option("spark.mongodb.read.database", "analytics") \
    .option("spark.mongodb.read.collection", "events") \
    .option("pipeline", '[{"$match": {"status": "active"}}, {"$project": {"user_id": 1, "amount": 1}}]') \
    .load()
```

### Writing to MongoDB

```python
# Batch writes with proper configuration
df.write \
    .format("mongodb") \
    .mode("append") \
    .option("spark.mongodb.write.connection.uri", "mongodb://localhost:27017") \
    .option("spark.mongodb.write.database", "prod") \
    .option("spark.mongodb.write.collection", "predictions") \
    .option("spark.mongodb.write.writeConcern.w", "majority") \
    .option("replaceDocument", "false") \
    .save()

# Upserts with stable keys
upsert_df.write \
    .format("mongodb") \
    .mode("append") \
    .option("spark.mongodb.write.connection.uri", "mongodb://localhost:27017") \
    .option("spark.mongodb.write.database", "prod") \
    .option("spark.mongodb.write.collection", "user_profiles") \
    .option("replaceDocument", "true") \
    .option("idFieldList", "user_id") \
    .save()
```

### Best Practices for MongoDB Integration

```python
# For large writes: Delta → MongoDB pattern
# 1. Write processed data to Delta first
processed_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/mnt/delta/processed_results")

# 2. Then push curated results to MongoDB
curated_df = spark.read.format("delta").load("/mnt/delta/processed_results")
curated_df.write \
    .format("mongodb") \
    .mode("append") \
    .option("spark.mongodb.write.connection.uri", "mongodb://localhost:27017") \
    .option("spark.mongodb.write.database", "prod") \
    .option("spark.mongodb.write.collection", "analytics_results") \
    .option("maxBatchSize", "1000") \
    .save()
```

## 🛡️ Reliability & Governance

### Medallion Architecture

```python
# Bronze Layer: Raw data ingestion
bronze_df = spark.read.format("json").load("/mnt/raw/events/")
bronze_df.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save("/mnt/delta/bronze/events")

# Silver Layer: Cleaned and validated data
silver_df = spark.read.format("delta").load("/mnt/delta/bronze/events") \
    .filter(col("event_time").isNotNull()) \
    .filter(col("user_id").isNotNull()) \
    .withColumn("processed_at", current_timestamp())

silver_df.write \
    .format("delta") \
    .mode("append") \
    .save("/mnt/delta/silver/events")

# Gold Layer: Business-ready aggregated data
gold_df = spark.read.format("delta").load("/mnt/delta/silver/events") \
    .groupBy("event_date", "event_type") \
    .agg(
        count("*").alias("event_count"),
        countDistinct("user_id").alias("unique_users")
    )

gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/mnt/delta/gold/daily_event_summary")
```

### Data Quality Checks

```python
# Implement data quality checks
def validate_data_quality(df):
    # Check for nulls in critical columns
    null_check = df.filter(
        col("user_id").isNull() | 
        col("event_time").isNull()
    ).count()
    
    if null_check > 0:
        raise ValueError(f"Found {null_check} records with null critical fields")
    
    # Check for reasonable value ranges
    invalid_amounts = df.filter(col("amount") < 0).count()
    if invalid_amounts > 0:
        raise ValueError(f"Found {invalid_amounts} records with negative amounts")
    
    return df

# Apply validation
validated_df = validate_data_quality(silver_df)
```

## 💰 Cost & Cluster Management

### Cluster Configuration

```python
# Right-size your cluster configuration
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Enable Photon for SQL/DataFrame workloads (Databricks)
spark.conf.set("spark.databricks.photon.enabled", "true")

# Autoscaling configuration
spark.conf.set("spark.databricks.cluster.autoScale.enabled", "true")
spark.conf.set("spark.databricks.cluster.autoScale.minWorkers", "2")
spark.conf.set("spark.databricks.cluster.autoScale.maxWorkers", "20")
```

### Monitoring and Alerting

```python
import mlflow

# Log important metrics
with mlflow.start_run():
    mlflow.log_metric("input_rows", df.count())
    mlflow.log_metric("processing_time_seconds", processing_time)
    mlflow.log_metric("output_files", output_file_count)
    
    # Log data quality metrics
    mlflow.log_metric("null_rate", null_count / total_count)
    mlflow.log_metric("duplicate_rate", duplicate_count / total_count)
```

## 🎯 Quick Reference

### Simple Rules of Thumb

| Scenario | Recommendation |
|----------|---------------|
| **Data Size < 2GB** | Use pandas/SQL on single node |
| **Joining/Aggregating 10M+ rows** | Use PySpark/SQL in Databricks |
| **Real-time streaming** | Use PySpark Structured Streaming |
| **Sub-100ms latency** | Use app services, not Spark |
| **Data Integration** | Write to Delta first, integrate outward |

### Performance Checklist

- [ ] Use Delta/Parquet over CSV/JSON
- [ ] Define schemas explicitly 
- [ ] Broadcast small tables (< 10MB)
- [ ] Enable Adaptive Query Execution (AQE)
- [ ] Partition on low-cardinality filter columns
- [ ] Target 128-512MB file sizes
- [ ] Cache only when reused 2+ times
- [ ] Use Pandas UDFs over Python UDFs
- [ ] Set appropriate watermarks for streaming
- [ ] Monitor cluster autoscaling

## 📁 Examples

Check out the `examples/` directory for complete working examples:

- [Basic ETL Pipeline](examples/basic_etl.py)
- [Streaming with Kafka](examples/kafka_streaming.py)
- [MongoDB Integration](examples/mongodb_integration.py)
- [Advanced Optimizations](examples/performance_optimization.py)
- [Data Quality Framework](examples/data_quality.py)

## 🤝 Contributing

Contributions are welcome! Please read our contributing guidelines and submit pull requests for any improvements.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.