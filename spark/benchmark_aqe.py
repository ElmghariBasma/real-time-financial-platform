from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg
import time

spark = (
    SparkSession.builder
    .appName("AQEBenchmark")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

SILVER_PATH = "/opt/spark-data/silver/transactions"

df = spark.read.format("delta").load(SILVER_PATH)

print("\n=== AQE BENCHMARK ===")
print(f"Nombre de lignes : {df.count()}")
print(f"AQE activé : {spark.conf.get('spark.sql.adaptive.enabled')}")

start = time.time()

result = (
    df.groupBy("type")
      .agg(
          count("*").alias("transaction_count"),
          sum("amount").alias("total_amount"),
          avg("amount").alias("average_amount")
      )
      .collect()
)

end = time.time()

print(f"Temps sans AQE : {end - start:.3f} secondes")
print(f"Résultat : {result}")

spark.stop()