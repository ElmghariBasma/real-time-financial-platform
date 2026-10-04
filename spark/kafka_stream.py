import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType
)



# ==================================================
# Configuration
# ==================================================

KAFKA_SERVER = "kafka:29092"
TOPIC_NAME = "financial-transactions"

# ==================================================
# Création de Spark
# ==================================================

spark = (
    SparkSession.builder
    .appName("FinancialTransactionsStreaming")
    .master("local[2]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "0.0.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Spark démarre...")
print(f"Spark version : {spark.version}")

# ==================================================
# Schéma d'une transaction PaySim
# ==================================================

transaction_schema = StructType([
    StructField("step", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("nameOrig", StringType(), True),
    StructField("nameDest", StringType(), True),
    StructField("oldbalanceOrg", DoubleType(), True),
    StructField("newbalanceOrig", DoubleType(), True),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("newbalanceDest", DoubleType(), True),
    StructField("isFraud", IntegerType(), True)
])

# ==================================================
# Lecture de Kafka
# ==================================================

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", TOPIC_NAME)
    .option("startingOffsets", "earliest")
    .load()
)

print("Connexion à Kafka établie.")

# ==================================================
# Transformation JSON → colonnes
# ==================================================

transactions = (
    raw_stream
    .select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("json_value")
    )
    .select(
        col("kafka_key"),
        from_json(
            col("json_value"),
            transaction_schema
        ).alias("transaction")
    )
    .select(
        col("kafka_key"),
        "transaction.*"
    )
)

# ==================================================
# Affichage des transactions
# ==================================================

BRONZE_PATH = "/opt/spark-data/bronze/transactions"

query = (
    transactions.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/opt/spark-data/checkpoints/bronze")
    .start(BRONZE_PATH)
)

print("Streaming démarré...")
print("En attente des transactions Kafka...")

query.awaitTermination()