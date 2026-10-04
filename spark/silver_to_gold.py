from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum,
    avg,
    max,
    when,
    round
)

spark = (
    SparkSession.builder
    .appName("SilverToGold")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

SILVER_PATH = "/opt/spark-data/silver/transactions"
GOLD_PATH = "/opt/spark-data/gold/transaction_metrics"

df = spark.read.format("delta").load(SILVER_PATH)

gold = (
    df.groupBy("type")
    .agg(
        count("*").alias("transaction_count"),

        round(
            sum("amount"),
            2
        ).alias("total_amount"),

        round(
            avg("amount"),
            2
        ).alias("average_amount"),

        round(
            max("amount"),
            2
        ).alias("max_amount"),

        sum(
            when(col("isFraud") == 1, 1).otherwise(0)
        ).alias("fraud_count")
    )
    .withColumn(
        "fraud_rate",
        round(
            col("fraud_count") / col("transaction_count") * 100,
            2
        )
    )
    .orderBy("transaction_count", ascending=False)
)

print("\n=== GOLD TRANSACTION METRICS ===")

gold.show(truncate=False)

(
    gold.write
    .format("delta")
    .mode("overwrite")
    .save(GOLD_PATH)
)

print("\nGold Layer créée avec succès.")

spark.stop()