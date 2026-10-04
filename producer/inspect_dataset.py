from pathlib import Path
import pandas as pd

# Chemin vers le fichier CSV
BASE_DIR = Path(__file__).resolve().parent.parent
DATASET_PATH = BASE_DIR / "data" / "PS_20174392719_1491204439457_log.csv"

# Lire seulement les 5 premières lignes
df = pd.read_csv(DATASET_PATH, nrows=5)

print("Colonnes du dataset :")
print(df.columns.tolist())

print("\nPremières lignes :")
print(df.to_string(index=False))