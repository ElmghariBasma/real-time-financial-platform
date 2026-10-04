from pathlib import Path
import json
import time

import pandas as pd
from kafka import KafkaProducer

BASE_DIR = Path(__file__).resolve().parent.parent

DATASET_PATH = (
    BASE_DIR
    / "data"
    / "PS_20174392719_1491204439457_log.csv"
)

KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "financial-transactions"

CHUNK_SIZE = 1000
MAX_TRANSACTIONS = 10000

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),

    # Optimisation du batching Kafka
    batch_size=32768,
    linger_ms=10,
    compression_type="gzip",
)

total_sent = 0

print("Démarrage du Producer optimisé...")
print(f"Taille d'un bloc : {CHUNK_SIZE}")
print(f"Nombre maximum de transactions : {MAX_TRANSACTIONS}")

start_time = time.time()

try:
    for chunk in pd.read_csv(DATASET_PATH, chunksize=CHUNK_SIZE):

        print(f"\nNouveau bloc chargé : {len(chunk)} lignes")

        for _, row in chunk.iterrows():

            if total_sent >= MAX_TRANSACTIONS:
                break

            transaction = {
                "step": int(row["step"]),
                "type": str(row["type"]),
                "amount": float(row["amount"]),
                "nameOrig": str(row["nameOrig"]),
                "nameDest": str(row["nameDest"]),
                "oldbalanceOrg": float(row["oldbalanceOrg"]),
                "newbalanceOrig": float(row["newbalanceOrig"]),
                "oldbalanceDest": float(row["oldbalanceDest"]),
                "newbalanceDest": float(row["newbalanceDest"]),
                "isFraud": int(row["isFraud"]),
            }

            key = transaction["nameOrig"]

            producer.send(
                TOPIC_NAME,
                key=key,
                value=transaction,
            )

            total_sent += 1

            print(
                f"[{total_sent}/{MAX_TRANSACTIONS}] "
                f"clé={key}"
            )

        if total_sent >= MAX_TRANSACTIONS:
            break

finally:
    # Attendre que tous les messages soient réellement envoyés
    producer.flush()
    producer.close()

    end_time = time.time()
    elapsed_time = end_time - start_time

    throughput = (
        total_sent / elapsed_time
        if elapsed_time > 0
        else 0
    )

    print(f"\nTemps total : {elapsed_time:.3f} secondes")
    print(f"Débit : {throughput:.2f} transactions/seconde")
    print(f"{total_sent} transactions envoyées.")
    print("Producer optimisé arrêté proprement.")