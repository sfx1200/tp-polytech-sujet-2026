"""
Utilitaires partagés par les scripts d'ingestion. `get_connection` est fourni tel quel ;
`fetch_csv` est à vous d'implémenter (cf. TODO)
"""
import os
import urllib.request

import duckdb
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BRONZE_DIR = os.path.join(BASE_DIR, "bronze")
DB_PATH = os.path.join(BASE_DIR, "warehouse.duckdb")
RAW_BASE = "https://raw.githubusercontent.com/kevinl75/tp-polytech-dataset/main"


def fetch_csv(subdir: str, filename: str) -> pd.DataFrame:
    """Doit renvoyer le contenu de <subdir>/<filename> sous forme de DataFrame pandas."""
    local_path = os.path.join(BRONZE_DIR, subdir, filename)

    # Bronze = copie brute : on écrit les octets reçus tels quels (pas de passage par pandas,
    # qui pourrait altérer le format). Si le fichier est déjà là, on ne le retélécharge pas.
    if not os.path.exists(local_path):
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        url = f"{RAW_BASE}/{subdir}/{filename}"
        with urllib.request.urlopen(url) as resp:
            content = resp.read()
        # Écriture dans un fichier temporaire puis renommage : un téléchargement interrompu ne
        # laisse jamais un fichier bronze tronqué qui serait ensuite pris pour valide.
        tmp_path = local_path + ".tmp"
        with open(tmp_path, "wb") as f:
            f.write(content)
        os.replace(tmp_path, local_path)

    return pd.read_csv(local_path)


def get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(DB_PATH)
