from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

spark = (
    SparkSession.builder
    .appName("BronzeToSilver")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

BRONZE_PATH = "/opt/spark-data/bronze/transactions"
SILVER_PATH = "/opt/spark-data/silver/transactions"

# Lire Bronze
df = spark.read.format("delta").load(BRONZE_PATH)

print("\n=== BRONZE ===")
print(f"Nombre de lignes : {df.count()}")

# Nettoyage et validation
silver = (
    df
    # Supprimer les lignes sans identifiant
    .filter(col("nameOrig").isNotNull())
    .filter(col("nameDest").isNotNull())

    # Montant valide
    .filter(col("amount") >= 0)

    # Types de transactions valides
    .filter(
        col("type").isin(
            "PAYMENT",
            "TRANSFER",
            "CASH_OUT",
            "DEBIT",
            "CASH_IN"
        )
    )

    # Éviter les doublons
    .dropDuplicates(["kafka_key"])

    # Feature engineering
    .withColumn(
        "balance_diff_orig",
        col("oldbalanceOrg") - col("newbalanceOrig")
    )
    .withColumn(
        "balance_diff_dest",
        col("newbalanceDest") - col("oldbalanceDest")
    )
    .withColumn(
        "amount_to_balance_ratio",
        when(
            col("oldbalanceOrg") > 0,
            col("amount") / col("oldbalanceOrg")
        ).otherwise(lit(0.0))
    )
)

print("\n=== SILVER ===")
print(f"Nombre de lignes : {silver.count()}")

silver.printSchema()

silver.show(10, truncate=False)

# Écriture Delta Silver
(
    silver.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("type")
    .save(SILVER_PATH)
)

print("\nSilver Layer créée avec succès.")

spark.stop()