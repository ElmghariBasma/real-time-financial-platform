from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count,
    countDistinct,
    min,
    max,
    avg
)

spark = (
    SparkSession.builder
    .appName("DiagnoseBronze")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

BRONZE_PATH = "/opt/spark-data/bronze/transactions"

df = spark.read.format("delta").load(BRONZE_PATH)

print("\n========================================")
print("DIAGNOSTIC BRONZE")
print("========================================")

total = df.count()
unique_keys = df.select("kafka_key").distinct().count()

print(f"Total lignes Bronze       : {total}")
print(f"Clés Kafka uniques        : {unique_keys}")
print(f"Doublons                  : {total - unique_keys}")

print("\n========================================")
print("NOMBRE D'OCCURRENCES PAR TRANSACTION")
print("========================================")

duplicates = (
    df.groupBy("kafka_key")
    .agg(count("*").alias("occurrences"))
    .orderBy("occurrences", ascending=False)
)

duplicates.show(20, truncate=False)

print("\n========================================")
print("STATISTIQUES DES OCCURRENCES")
print("========================================")

duplicates.select(
    min("occurrences").alias("min_occurrences"),
    max("occurrences").alias("max_occurrences"),
    avg("occurrences").alias("avg_occurrences")
).show()

print("\n========================================")
print("REPARTITION PAR TYPE")
print("========================================")

df.groupBy("type").count().orderBy("type").show()

spark.stop()