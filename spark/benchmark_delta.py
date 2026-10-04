from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

spark = (
    SparkSession.builder
    .appName("DeltaBenchmark")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

SILVER_PATH = "/opt/spark-data/silver/transactions"

df = spark.read.format("delta").load(SILVER_PATH)

print("\n=== DELTA BENCHMARK ===")

print(f"Nombre de lignes : {df.count()}")
df.filter(col("type") == "TRANSFER").explain(True)

print("\n=== REQUÊTE ===")

start = time.time()

result = (
    df
    .select("type")
    .filter(col("type") == "TRANSFER")
    .groupBy("type")
    .count()
    .collect()
)

end = time.time()

print(f"Temps de lecture : {end - start:.3f} secondes")
print(f"Résultat : {result}")

spark.stop()