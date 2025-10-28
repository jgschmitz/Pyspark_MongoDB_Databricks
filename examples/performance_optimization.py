#!/usr/bin/env python3
"""
Performance Optimization Examples
==================================

This example demonstrates advanced performance optimization techniques
for PySpark applications including AQE, caching strategies, and monitoring.

Author: PySpark MongoDB Databricks Examples
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

def create_optimized_spark_session():
    """Create Spark session with performance optimizations"""
    return SparkSession.builder \
        .appName("PerformanceOptimization") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB") \
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000") \
        .getOrCreate()

def benchmark_join_strategies():
    """
    Compare different join strategies and their performance
    """
    spark = create_optimized_spark_session()
    
    print("🚀 Benchmarking Join Strategies...")
    
    # Create sample data
    large_df = spark.range(1000000).select(
        col("id").alias("customer_id"),
        (rand() * 1000).cast("int").alias("amount"),
        current_date().alias("order_date")
    )
    
    small_df = spark.range(1000).select(
        col("id").alias("customer_id"),
        concat(lit("Customer_"), col("id")).alias("customer_name"),
        (rand() * 5).cast("int").alias("tier")
    )
    
    # Strategy 1: Default join (shuffle-based)
    start_time = time.time()
    default_join = large_df.join(small_df, "customer_id")
    result1_count = default_join.count()
    time1 = time.time() - start_time
    
    print(f"Default Join: {result1_count} records in {time1:.2f}s")
    
    # Strategy 2: Broadcast join
    start_time = time.time()
    broadcast_join = large_df.join(broadcast(small_df), "customer_id")
    result2_count = broadcast_join.count()
    time2 = time.time() - start_time
    
    print(f"Broadcast Join: {result2_count} records in {time2:.2f}s")
    print(f"Performance improvement: {((time1-time2)/time1)*100:.1f}%")
    
    spark.stop()

def demonstrate_caching_strategies():
    """
    Show effective caching strategies and their impact
    """
    spark = create_optimized_spark_session()
    
    print("💾 Demonstrating Caching Strategies...")
    
    # Create expensive computation
    df = spark.range(100000).select(
        col("id"),
        (rand() * 100).alias("value1"),
        (rand() * 50).alias("value2")
    )
    
    # Expensive transformation
    expensive_df = df.withColumn(
        "complex_calc",
        sqrt(col("value1")) + sin(col("value2")) + cos(col("value1") * col("value2"))
    ).filter(col("complex_calc") > 5)
    
    # Without caching - multiple actions
    print("Without caching:")
    start_time = time.time()
    count1 = expensive_df.count()
    sum_result = expensive_df.agg(sum("complex_calc")).collect()[0][0]
    avg_result = expensive_df.agg(avg("complex_calc")).collect()[0][0]
    time_without_cache = time.time() - start_time
    
    print(f"Multiple operations without cache: {time_without_cache:.2f}s")
    
    # With caching - multiple actions
    print("With caching:")
    expensive_df.cache()  # Cache the DataFrame
    
    start_time = time.time()
    count2 = expensive_df.count()  # Triggers caching
    sum_result_cached = expensive_df.agg(sum("complex_calc")).collect()[0][0]
    avg_result_cached = expensive_df.agg(avg("complex_calc")).collect()[0][0]
    time_with_cache = time.time() - start_time
    
    print(f"Multiple operations with cache: {time_with_cache:.2f}s")
    print(f"Cache performance improvement: {((time_without_cache-time_with_cache)/time_without_cache)*100:.1f}%")
    
    # Always unpersist when done
    expensive_df.unpersist()
    spark.stop()

def optimize_partitioning():
    """
    Demonstrate partitioning optimization techniques
    """
    spark = create_optimized_spark_session()
    
    print("🗂️ Optimizing Partitioning...")
    
    # Create sample data with skewed distribution
    df = spark.range(100000).select(
        col("id"),
        (when(col("id") % 100 == 0, 1).otherwise((rand() * 10).cast("int"))).alias("category"),
        (rand() * 1000).alias("amount")
    )
    
    print(f"Original partitions: {df.rdd.getNumPartitions()}")
    
    # Check partition distribution
    partition_counts = df.groupBy(spark_partition_id()).count().collect()
    print("Partition distribution:")
    for row in partition_counts:
        print(f"  Partition {row[0]}: {row[1]} records")
    
    # Optimize partitioning for joins
    optimized_df = df.repartition(col("category"))
    print(f"After repartitioning by category: {optimized_df.rdd.getNumPartitions()}")
    
    # For final output, optimize file sizes
    coalesced_df = optimized_df.coalesce(4)
    print(f"After coalescing for output: {coalesced_df.rdd.getNumPartitions()}")
    
    spark.stop()

def pandas_udf_vs_python_udf():
    """
    Compare performance of Pandas UDFs vs regular Python UDFs
    """
    spark = create_optimized_spark_session()
    
    print("🐼 Comparing UDF Performance...")
    
    # Sample data
    df = spark.range(50000).select(
        col("id"),
        (rand() * 100).alias("value")
    )
    
    # Regular Python UDF
    from pyspark.sql.functions import udf
    from pyspark.sql.types import DoubleType
    
    def complex_calculation(x):
        import math
        return math.sqrt(x) + math.sin(x) + math.cos(x)
    
    python_udf = udf(complex_calculation, DoubleType())
    
    # Pandas UDF (vectorized)
    from pyspark.sql.functions import pandas_udf
    import pandas as pd
    import numpy as np
    
    @pandas_udf(returnType=DoubleType())
    def vectorized_calculation(values: pd.Series) -> pd.Series:
        return np.sqrt(values) + np.sin(values) + np.cos(values)
    
    # Benchmark Python UDF
    start_time = time.time()
    result_python = df.withColumn("result", python_udf(col("value")))
    count_python = result_python.count()
    time_python = time.time() - start_time
    
    print(f"Python UDF: {count_python} records in {time_python:.2f}s")
    
    # Benchmark Pandas UDF
    start_time = time.time()
    result_pandas = df.withColumn("result", vectorized_calculation(col("value")))
    count_pandas = result_pandas.count()
    time_pandas = time.time() - start_time
    
    print(f"Pandas UDF: {count_pandas} records in {time_pandas:.2f}s")
    print(f"Pandas UDF speedup: {time_python/time_pandas:.1f}x faster")
    
    spark.stop()

def monitor_spark_performance():
    """
    Demonstrate performance monitoring and metrics collection
    """
    spark = create_optimized_spark_session()
    
    print("📊 Monitoring Spark Performance...")
    
    # Enable metrics
    spark.sparkContext.statusTracker()
    
    # Sample workload
    df = spark.range(100000).select(
        col("id"),
        (rand() * 10).cast("int").alias("category"),
        (rand() * 1000).alias("amount")
    )
    
    # Complex aggregation
    result = df.groupBy("category").agg(
        count("*").alias("count"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount"),
        stddev("amount").alias("stddev_amount")
    )
    
    # Collect metrics before action
    start_time = time.time()
    
    # Execute action and collect results
    results = result.collect()
    execution_time = time.time() - start_time
    
    # Get job metrics
    status_tracker = spark.sparkContext.statusTracker()
    job_ids = status_tracker.getJobIdsForGroup(None)
    
    print(f"Execution time: {execution_time:.2f}s")
    print(f"Number of jobs: {len(job_ids) if job_ids else 0}")
    
    # Application metrics
    print("Application Metrics:")
    print(f"  Total cores: {spark.sparkContext.defaultParallelism}")
    print(f"  Executor memory: {spark.conf.get('spark.executor.memory', 'default')}")
    print(f"  Driver memory: {spark.conf.get('spark.driver.memory', 'default')}")
    
    # Print results
    print("\nAggregation Results:")
    for row in results:
        print(f"  Category {row['category']}: {row['count']} records, "
              f"avg amount: ${row['avg_amount']:.2f}")
    
    spark.stop()

def adaptive_query_execution_demo():
    """
    Demonstrate Adaptive Query Execution (AQE) benefits
    """
    print("🔄 Adaptive Query Execution Demo...")
    
    # Without AQE
    spark_no_aqe = SparkSession.builder \
        .appName("NoAQE") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()
    
    df1 = spark_no_aqe.range(100000).select(col("id"), (col("id") % 100).alias("group"))
    df2 = spark_no_aqe.range(1000).select(col("id"), concat(lit("Group_"), col("id")).alias("name"))
    
    start_time = time.time()
    result_no_aqe = df1.join(df2, df1.group == df2.id).count()
    time_no_aqe = time.time() - start_time
    
    spark_no_aqe.stop()
    
    # With AQE
    spark_aqe = SparkSession.builder \
        .appName("WithAQE") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .getOrCreate()
    
    df1_aqe = spark_aqe.range(100000).select(col("id"), (col("id") % 100).alias("group"))
    df2_aqe = spark_aqe.range(1000).select(col("id"), concat(lit("Group_"), col("id")).alias("name"))
    
    start_time = time.time()
    result_aqe = df1_aqe.join(df2_aqe, df1_aqe.group == df2_aqe.id).count()
    time_aqe = time.time() - start_time
    
    print(f"Without AQE: {result_no_aqe} records in {time_no_aqe:.2f}s")
    print(f"With AQE: {result_aqe} records in {time_aqe:.2f}s")
    print(f"AQE improvement: {((time_no_aqe-time_aqe)/time_no_aqe)*100:.1f}%")
    
    spark_aqe.stop()

def file_format_performance():
    """
    Compare performance across different file formats
    """
    spark = create_optimized_spark_session()
    
    print("📁 File Format Performance Comparison...")
    
    # Create sample data
    df = spark.range(50000).select(
        col("id"),
        concat(lit("user_"), col("id")).alias("username"),
        (rand() * 1000).cast("double").alias("amount"),
        current_timestamp().alias("timestamp")
    )
    
    formats = ["json", "csv", "parquet"]
    write_times = {}
    read_times = {}
    file_sizes = {}
    
    for fmt in formats:
        # Write performance
        path = f"/tmp/test_data_{fmt}"
        start_time = time.time()
        df.write.mode("overwrite").format(fmt).save(path)
        write_times[fmt] = time.time() - start_time
        
        # Read performance  
        start_time = time.time()
        read_df = spark.read.format(fmt).load(path)
        count = read_df.count()
        read_times[fmt] = time.time() - start_time
        
        print(f"{fmt.upper()}:")
        print(f"  Write: {write_times[fmt]:.2f}s")
        print(f"  Read: {read_times[fmt]:.2f}s")
        print(f"  Records: {count}")
    
    # Show performance comparison
    print("\nPerformance Summary:")
    fastest_write = min(write_times, key=write_times.get)
    fastest_read = min(read_times, key=read_times.get)
    
    print(f"Fastest write: {fastest_write} ({write_times[fastest_write]:.2f}s)")
    print(f"Fastest read: {fastest_read} ({read_times[fastest_read]:.2f}s)")
    
    spark.stop()

if __name__ == "__main__":
    print("🚀 PySpark Performance Optimization Examples")
    
    examples = [
        ("Join Strategies", benchmark_join_strategies),
        ("Caching Strategies", demonstrate_caching_strategies), 
        ("Partitioning Optimization", optimize_partitioning),
        ("UDF Performance", pandas_udf_vs_python_udf),
        ("Performance Monitoring", monitor_spark_performance),
        ("Adaptive Query Execution", adaptive_query_execution_demo),
        ("File Format Performance", file_format_performance)
    ]
    
    for name, func in examples:
        print(f"\n{'='*50}")
        print(f"Running: {name}")
        try:
            func()
            print(f"✅ {name} completed successfully!")
        except Exception as e:
            print(f"❌ {name} failed: {str(e)}")
    
    print("\n🎉 All performance examples completed!")