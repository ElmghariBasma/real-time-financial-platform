from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count,
    sum,
    avg,
    max,
    when,
    round
)

spark = (
    SparkSession.builder
    .appName("AnalyzeSilver")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

SILVER_PATH = "/opt/spark-data/silver/transactions"

df = spark.read.format("delta").load(SILVER_PATH)

print("\n=== SILVER ANALYSIS ===")

print(f"Total transactions : {df.count()}")

print("\n=== TRANSACTIONS BY TYPE ===")

by_type = (
    df.groupBy("type")
    .agg(
        count("*").alias("transaction_count"),
        round(sum("amount"), 2).alias("total_amount"),
        round(avg("amount"), 2).alias("average_amount"),
        round(max("amount"), 2).alias("max_amount"),
        sum(when(df.isFraud == 1, 1).otherwise(0)).alias("fraud_count")
    )
    .orderBy("transaction_count", ascending=False)
)

by_type.show(truncate=False)

print("\n=== FRAUD SUMMARY ===")

fraud_summary = (
    df.groupBy("isFraud")
    .count()
    .orderBy("isFraud")
)

fraud_summary.show()

spark.stop()