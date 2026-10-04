from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CheckBronze")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.format("delta").load(
    "/opt/spark-data/bronze/transactions"
)

count = df.count()

print(f"BRONZE_COUNT={count}")

spark.stop()