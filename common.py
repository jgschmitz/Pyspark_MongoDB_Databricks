from pyspark.sql import SparkSession

def get_spark(app_name="pyspark-mongo-demo"):
    """
    Create/return Spark with MongoDB configs pulled from Databricks secrets.
    Swap these for your secret scopes & keys.
    """
    # Example: dbutils.secrets.get("scope","key")
    MONGO_URI = dbutils.secrets.get("mongo", "uri")          # e.g. "mongodb+srv://user:pwd@cluster/?retryWrites=true&w=majority"
    DB         = dbutils.secrets.get("mongo", "database")    # e.g. "demo"
    READ_COLL  = dbutils.secrets.get("mongo", "read_coll")   # e.g. "events"
    WRITE_COLL = dbutils.secrets.get("mongo", "write_coll")  # e.g. "predictions"

    spark = (SparkSession.getActiveSession()
             or SparkSession.builder.appName(app_name).getOrCreate())

    spark.conf.set("spark.mongodb.read.connection.uri", MONGO_URI)
    spark.conf.set("spark.mongodb.write.connection.uri", MONGO_URI)
    spark.conf.set("spark.mongodb.read.database", DB)
    spark.conf.set("spark.mongodb.write.database", DB)
    spark.conf.set("spark.mongodb.read.collection", READ_COLL)
    spark.conf.set("spark.mongodb.write.collection", WRITE_COLL)

    # Useful perf flags
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    return spark
