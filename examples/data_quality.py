#!/usr/bin/env python3
"""
Data Quality Framework
======================

This example demonstrates a comprehensive data quality framework for PySpark,
including validation rules, data profiling, and automated quality checks.

Author: PySpark MongoDB Databricks Examples
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import Dict, List, Any
import json

def create_spark_session():
    """Create Spark session with quality framework configuration"""
    return SparkSession.builder \
        .appName("DataQualityFramework") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

class DataQualityChecker:
    """
    Comprehensive data quality checker for PySpark DataFrames
    """
    
    def __init__(self, spark_session=None):
        self.spark = spark_session or create_spark_session()
        self.quality_results = []
    
    def profile_dataframe(self, df, sample_size=10000):
        """
        Generate comprehensive data profile for a DataFrame
        """
        print("📊 Profiling DataFrame...")
        
        # Basic statistics
        total_records = df.count()
        total_columns = len(df.columns)
        
        print(f"Dataset Overview:")
        print(f"  Total Records: {total_records:,}")
        print(f"  Total Columns: {total_columns}")
        
        # Column-level analysis
        profile = {
            "total_records": total_records,
            "total_columns": total_columns,
            "columns": {}
        }
        
        for col_name in df.columns:
            col_profile = self._profile_column(df, col_name, sample_size)
            profile["columns"][col_name] = col_profile
            
            print(f"\nColumn: {col_name}")
            print(f"  Type: {col_profile['data_type']}")
            print(f"  Null Count: {col_profile['null_count']:,} ({col_profile['null_percentage']:.2f}%)")
            print(f"  Distinct Values: {col_profile['distinct_count']:,}")
            print(f"  Cardinality: {col_profile['cardinality']:.4f}")
            
            if col_profile['data_type'] in ['int', 'bigint', 'double', 'float']:
                stats = col_profile.get('numeric_stats', {})
                print(f"  Min: {stats.get('min', 'N/A')}")
                print(f"  Max: {stats.get('max', 'N/A')}")
                print(f"  Mean: {stats.get('mean', 'N/A')}")
                print(f"  Std Dev: {stats.get('stddev', 'N/A')}")
        
        return profile
    
    def _profile_column(self, df, col_name, sample_size):
        """Profile individual column"""
        col_type = dict(df.dtypes)[col_name]
        total_records = df.count()
        
        # Basic statistics
        null_count = df.filter(col(col_name).isNull()).count()
        distinct_count = df.select(col_name).distinct().count()
        
        profile = {
            "data_type": col_type,
            "null_count": null_count,
            "null_percentage": (null_count / total_records) * 100,
            "distinct_count": distinct_count,
            "cardinality": distinct_count / total_records if total_records > 0 else 0
        }
        
        # Numeric statistics
        if col_type in ['int', 'bigint', 'double', 'float', 'decimal']:
            numeric_stats = df.select(
                min(col_name).alias('min'),
                max(col_name).alias('max'),
                mean(col_name).alias('mean'),
                stddev(col_name).alias('stddev')
            ).collect()[0]
            
            profile["numeric_stats"] = {
                "min": numeric_stats['min'],
                "max": numeric_stats['max'],
                "mean": numeric_stats['mean'],
                "stddev": numeric_stats['stddev']
            }
        
        # Sample values for inspection
        sample_values = df.select(col_name).limit(sample_size).rdd.map(lambda x: x[0]).collect()
        profile["sample_values"] = sample_values[:10]  # First 10 for display
        
        return profile
    
    def check_completeness(self, df, required_columns: List[str], threshold: float = 0.95):
        """
        Check data completeness - ensure required columns have sufficient non-null values
        """
        print("🔍 Checking Data Completeness...")
        
        total_records = df.count()
        completeness_results = []
        
        for col_name in required_columns:
            if col_name not in df.columns:
                result = {
                    "column": col_name,
                    "status": "FAILED",
                    "reason": "Column not found",
                    "completeness_rate": 0.0
                }
            else:
                non_null_count = df.filter(col(col_name).isNotNull()).count()
                completeness_rate = non_null_count / total_records if total_records > 0 else 0
                
                result = {
                    "column": col_name,
                    "status": "PASSED" if completeness_rate >= threshold else "FAILED",
                    "completeness_rate": completeness_rate,
                    "non_null_count": non_null_count,
                    "total_records": total_records,
                    "threshold": threshold
                }
            
            completeness_results.append(result)
            status_icon = "✅" if result["status"] == "PASSED" else "❌"
            print(f"  {status_icon} {col_name}: {result['completeness_rate']:.2%} complete")
        
        self.quality_results.append({
            "check_type": "completeness",
            "results": completeness_results
        })
        
        return completeness_results
    
    def check_uniqueness(self, df, unique_columns: List[str]):
        """
        Check uniqueness constraints - ensure specified columns have unique values
        """
        print("🔑 Checking Uniqueness Constraints...")
        
        uniqueness_results = []
        total_records = df.count()
        
        for col_name in unique_columns:
            if col_name not in df.columns:
                result = {
                    "column": col_name,
                    "status": "FAILED",
                    "reason": "Column not found"
                }
            else:
                distinct_count = df.select(col_name).distinct().count()
                duplicate_count = total_records - distinct_count
                
                result = {
                    "column": col_name,
                    "status": "PASSED" if duplicate_count == 0 else "FAILED",
                    "total_records": total_records,
                    "distinct_count": distinct_count,
                    "duplicate_count": duplicate_count,
                    "uniqueness_rate": distinct_count / total_records if total_records > 0 else 0
                }
            
            uniqueness_results.append(result)
            status_icon = "✅" if result["status"] == "PASSED" else "❌"
            print(f"  {status_icon} {col_name}: {result.get('duplicate_count', 'N/A')} duplicates")
        
        self.quality_results.append({
            "check_type": "uniqueness", 
            "results": uniqueness_results
        })
        
        return uniqueness_results
    
    def check_validity(self, df, validation_rules: Dict[str, Any]):
        """
        Check data validity using custom validation rules
        
        validation_rules example:
        {
            "age": {"min": 0, "max": 120},
            "email": {"pattern": r"^[\\w\\.-]+@[\\w\\.-]+\\.[a-zA-Z]{2,}$"},
            "status": {"values": ["active", "inactive", "pending"]}
        }
        """
        print("✅ Checking Data Validity...")
        
        validity_results = []
        
        for col_name, rules in validation_rules.items():
            if col_name not in df.columns:
                result = {
                    "column": col_name,
                    "status": "FAILED",
                    "reason": "Column not found"
                }
                validity_results.append(result)
                continue
            
            # Apply validation rules
            valid_df = df
            invalid_conditions = []
            
            # Range validation
            if "min" in rules:
                invalid_conditions.append(col(col_name) < rules["min"])
            if "max" in rules:
                invalid_conditions.append(col(col_name) > rules["max"])
            
            # Pattern validation (regex)
            if "pattern" in rules:
                invalid_conditions.append(~col(col_name).rlike(rules["pattern"]))
            
            # Value set validation
            if "values" in rules:
                invalid_conditions.append(~col(col_name).isin(rules["values"]))
            
            # Count invalid records
            if invalid_conditions:
                invalid_condition = invalid_conditions[0]
                for condition in invalid_conditions[1:]:
                    invalid_condition = invalid_condition | condition
                
                invalid_count = df.filter(invalid_condition & col(col_name).isNotNull()).count()
                total_non_null = df.filter(col(col_name).isNotNull()).count()
                validity_rate = (total_non_null - invalid_count) / total_non_null if total_non_null > 0 else 1.0
                
                result = {
                    "column": col_name,
                    "status": "PASSED" if invalid_count == 0 else "FAILED",
                    "total_records": total_non_null,
                    "invalid_count": invalid_count,
                    "validity_rate": validity_rate,
                    "rules": rules
                }
            else:
                result = {
                    "column": col_name,
                    "status": "PASSED",
                    "reason": "No validation rules applied"
                }
            
            validity_results.append(result)
            status_icon = "✅" if result["status"] == "PASSED" else "❌"
            invalid_count = result.get("invalid_count", 0)
            print(f"  {status_icon} {col_name}: {invalid_count} invalid records")
        
        self.quality_results.append({
            "check_type": "validity",
            "results": validity_results
        })
        
        return validity_results
    
    def check_consistency(self, df, consistency_rules: List[Dict]):
        """
        Check data consistency using business rules
        
        consistency_rules example:
        [
            {
                "name": "end_date_after_start_date",
                "condition": "end_date >= start_date",
                "description": "End date should be after start date"
            }
        ]
        """
        print("🔄 Checking Data Consistency...")
        
        consistency_results = []
        
        for rule in consistency_rules:
            rule_name = rule["name"]
            condition = rule["condition"]
            description = rule.get("description", "")
            
            try:
                # Count records that violate the consistency rule
                violation_count = df.filter(f"NOT ({condition})").count()
                total_count = df.count()
                consistency_rate = (total_count - violation_count) / total_count if total_count > 0 else 1.0
                
                result = {
                    "rule_name": rule_name,
                    "status": "PASSED" if violation_count == 0 else "FAILED",
                    "description": description,
                    "total_records": total_count,
                    "violation_count": violation_count,
                    "consistency_rate": consistency_rate
                }
                
            except Exception as e:
                result = {
                    "rule_name": rule_name,
                    "status": "ERROR",
                    "reason": str(e)
                }
            
            consistency_results.append(result)
            status_icon = "✅" if result["status"] == "PASSED" else "❌" if result["status"] == "FAILED" else "⚠️"
            violation_count = result.get("violation_count", 0)
            print(f"  {status_icon} {rule_name}: {violation_count} violations")
        
        self.quality_results.append({
            "check_type": "consistency",
            "results": consistency_results
        })
        
        return consistency_results
    
    def detect_anomalies(self, df, numeric_columns: List[str], z_threshold: float = 3.0):
        """
        Detect statistical anomalies in numeric columns using Z-score
        """
        print("🚨 Detecting Anomalies...")
        
        anomaly_results = []
        
        for col_name in numeric_columns:
            if col_name not in df.columns:
                continue
            
            # Calculate statistics
            stats = df.select(
                mean(col_name).alias("mean"),
                stddev(col_name).alias("stddev")
            ).collect()[0]
            
            col_mean = stats["mean"]
            col_stddev = stats["stddev"]
            
            if col_stddev is None or col_stddev == 0:
                continue
            
            # Calculate Z-scores and find anomalies
            df_with_zscore = df.withColumn(
                "z_score",
                abs((col(col_name) - lit(col_mean)) / lit(col_stddev))
            )
            
            anomaly_count = df_with_zscore.filter(col("z_score") > z_threshold).count()
            total_count = df.filter(col(col_name).isNotNull()).count()
            
            # Get sample anomalies
            sample_anomalies = df_with_zscore.filter(col("z_score") > z_threshold) \
                .select(col_name, "z_score") \
                .limit(5) \
                .collect()
            
            result = {
                "column": col_name,
                "anomaly_count": anomaly_count,
                "total_records": total_count,
                "anomaly_rate": anomaly_count / total_count if total_count > 0 else 0,
                "z_threshold": z_threshold,
                "mean": col_mean,
                "stddev": col_stddev,
                "sample_anomalies": [(row[col_name], row["z_score"]) for row in sample_anomalies]
            }
            
            anomaly_results.append(result)
            print(f"  📊 {col_name}: {anomaly_count} anomalies ({result['anomaly_rate']:.2%})")
        
        self.quality_results.append({
            "check_type": "anomalies",
            "results": anomaly_results
        })
        
        return anomaly_results
    
    def generate_quality_report(self):
        """
        Generate comprehensive data quality report
        """
        print("📋 Generating Quality Report...")
        
        report = {
            "timestamp": str(current_timestamp()),
            "summary": {
                "total_checks": len(self.quality_results),
                "checks_passed": 0,
                "checks_failed": 0,
                "overall_score": 0.0
            },
            "detailed_results": self.quality_results
        }
        
        # Calculate summary statistics
        total_checks = 0
        passed_checks = 0
        
        for check_result in self.quality_results:
            if "results" in check_result:
                for result in check_result["results"]:
                    total_checks += 1
                    if result.get("status") == "PASSED":
                        passed_checks += 1
        
        report["summary"]["checks_passed"] = passed_checks
        report["summary"]["checks_failed"] = total_checks - passed_checks
        report["summary"]["overall_score"] = passed_checks / total_checks if total_checks > 0 else 0.0
        
        print(f"\n📊 Quality Report Summary:")
        print(f"  Total Checks: {total_checks}")
        print(f"  Passed: {passed_checks} ✅")
        print(f"  Failed: {total_checks - passed_checks} ❌")
        print(f"  Overall Score: {report['summary']['overall_score']:.2%}")
        
        return report

def example_comprehensive_quality_check():
    """
    Comprehensive example of data quality checking
    """
    spark = create_spark_session()
    quality_checker = DataQualityChecker(spark)
    
    # Create sample dataset with quality issues
    sample_data = [
        (1, "john@email.com", 25, "2023-01-01", "2023-06-01", "active", 50000),
        (2, "jane.doe@company.com", 30, "2023-01-15", "2023-12-31", "active", 75000),
        (None, "invalid-email", -5, "2023-02-01", "2022-12-01", "invalid_status", 200000),  # Issues
        (4, "bob@test.com", 45, "2023-03-01", "2023-09-01", "inactive", 60000),
        (2, "duplicate@email.com", 35, "2023-04-01", "2023-10-01", "pending", 80000),  # Duplicate ID
        (6, "alice@example.com", 28, "2023-05-01", None, "active", 55000),  # Missing end_date
    ]
    
    schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("email", StringType(), True),
        StructField("age", IntegerType(), True), 
        StructField("start_date", StringType(), True),
        StructField("end_date", StringType(), True),
        StructField("status", StringType(), True),
        StructField("salary", IntegerType(), True)
    ])
    
    df = spark.createDataFrame(sample_data, schema)
    
    print("🔍 Starting Comprehensive Data Quality Check...")
    print("="*60)
    
    # 1. Profile the data
    profile = quality_checker.profile_dataframe(df)
    
    print("\n" + "="*60)
    
    # 2. Check completeness
    required_columns = ["user_id", "email", "age", "start_date", "status"]
    completeness_results = quality_checker.check_completeness(df, required_columns, threshold=0.9)
    
    print("\n" + "="*60)
    
    # 3. Check uniqueness
    unique_columns = ["user_id", "email"]
    uniqueness_results = quality_checker.check_uniqueness(df, unique_columns)
    
    print("\n" + "="*60)
    
    # 4. Check validity
    validation_rules = {
        "age": {"min": 0, "max": 100},
        "email": {"pattern": r"^[\w\.-]+@[\w\.-]+\.[a-zA-Z]{2,}$"},
        "status": {"values": ["active", "inactive", "pending"]},
        "salary": {"min": 20000, "max": 150000}
    }
    validity_results = quality_checker.check_validity(df, validation_rules)
    
    print("\n" + "="*60)
    
    # 5. Check consistency  
    consistency_rules = [
        {
            "name": "end_date_after_start_date",
            "condition": "end_date IS NULL OR end_date >= start_date",
            "description": "End date should be after or equal to start date"
        }
    ]
    consistency_results = quality_checker.check_consistency(df, consistency_rules)
    
    print("\n" + "="*60)
    
    # 6. Detect anomalies
    numeric_columns = ["age", "salary"]
    anomaly_results = quality_checker.detect_anomalies(df, numeric_columns)
    
    print("\n" + "="*60)
    
    # 7. Generate final report
    final_report = quality_checker.generate_quality_report()
    
    spark.stop()
    return final_report

def create_data_quality_pipeline():
    """
    Example of integrating data quality checks into a data pipeline
    """
    spark = create_spark_session()
    
    print("🔧 Creating Data Quality Pipeline...")
    
    def quality_gate(df, stage_name):
        """Quality gate function that can be inserted into pipelines"""
        print(f"\n🚪 Quality Gate: {stage_name}")
        quality_checker = DataQualityChecker(spark)
        
        # Basic quality checks
        completeness = quality_checker.check_completeness(df, df.columns, threshold=0.8)
        
        # Check if quality gate passes
        failed_checks = [c for c in completeness if c["status"] == "FAILED"]
        
        if failed_checks:
            print(f"❌ Quality gate FAILED for {stage_name}")
            for check in failed_checks:
                print(f"  - {check['column']}: {check['completeness_rate']:.2%} complete")
            raise ValueError(f"Quality gate failed for stage: {stage_name}")
        else:
            print(f"✅ Quality gate PASSED for {stage_name}")
        
        return df
    
    # Example pipeline with quality gates
    try:
        # Stage 1: Raw data ingestion
        raw_df = spark.createDataFrame([
            (1, "user1", 100.0),
            (2, "user2", 200.0),
            (3, "user3", None)  # Missing value
        ], ["id", "name", "amount"])
        
        raw_df = quality_gate(raw_df, "Raw Data Ingestion")
        
        # Stage 2: Data transformation
        transformed_df = raw_df.withColumn("amount_doubled", col("amount") * 2)
        transformed_df = quality_gate(transformed_df, "Data Transformation") 
        
        # Stage 3: Final output
        final_df = transformed_df.filter(col("amount").isNotNull())
        final_df = quality_gate(final_df, "Final Output")
        
        print("✅ Pipeline completed successfully with all quality gates passed!")
        
    except ValueError as e:
        print(f"💥 Pipeline failed at quality gate: {e}")
    
    spark.stop()

if __name__ == "__main__":
    print("🚀 Data Quality Framework Examples")
    
    print("\n" + "="*60)
    print("Example 1: Comprehensive Quality Check")
    try:
        report = example_comprehensive_quality_check()
        print("✅ Comprehensive quality check completed!")
    except Exception as e:
        print(f"❌ Quality check failed: {str(e)}")
    
    print("\n" + "="*60)
    print("Example 2: Data Quality Pipeline Integration")
    try:
        create_data_quality_pipeline()
        print("✅ Quality pipeline example completed!")
    except Exception as e:
        print(f"❌ Quality pipeline failed: {str(e)}")
    
    print("\n🎉 All data quality examples completed!")