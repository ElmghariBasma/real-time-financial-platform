from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

spark = (
    SparkSession.builder
    .appName("PartitioningBenchmark")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

SILVER_PATH = "/opt/spark-data/silver/transactions"

df = spark.read.format("delta").load(SILVER_PATH)

print("\n=== DATASET ===")
print(f"Nombre de lignes : {df.count()}")
print(f"Nombre de partitions initiales : {df.rdd.getNumPartitions()}")

# Benchmark avec repartition
df_coalesced = df.coalesce(2)

print("\n=== APRÈS COALESCE ===")
print(f"Nombre de partitions : {df_coalesced.rdd.getNumPartitions()}")

start = time.time()

result = (
    df_coalesced.groupBy("type")
    .count()
    .collect()
)

end = time.time()

print(f"Temps : {end - start:.3f} secondes")
print(f"Résultat : {result}")

spark.stop()