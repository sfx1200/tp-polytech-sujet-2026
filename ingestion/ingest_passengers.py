



"""
À compléter : ingestion de l'entité `passengers`, découpée en 3 fonctions bronze/silver/gold
(même structure que `ingest_airports.py`, à utiliser comme modèle).
"""
from datetime import date

from common import fetch_csv, get_connection

LANGS = ["en", "fr"]
PASSENGER_COLS = [
    "first_name", "last_name", "gender", "nationality", "email",
    "birth_date", "signup_date", "source_system",
]


def _snapshot_file(lang: str, day: date = None, init: bool = False):
    if init:
        return "init", f"passengers_{lang}.csv"
    assert day is not None
    return "2025-09", f"passengers_{lang}_{day.isoformat()}.csv"


def ingest_bronze(day: date = None, init: bool = False):
    """Télécharge les deux snapshots (EN et FR) du jour (ou de init/) vers bronze/."""
    for lang in LANGS:
        subdir, filename = _snapshot_file(lang, day, init)
        fetch_csv(subdir, filename)


def create_silver_table(con):
    # Schéma unique pour les deux CRM : noms de colonnes EN, genre en Male/Female, dates typées.
    # source_system garde la trace du CRM d'origine (utile pour le debug).
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_passengers (
            passenger_id VARCHAR PRIMARY KEY,
            first_name VARCHAR,
            last_name VARCHAR,
            gender VARCHAR,
            nationality VARCHAR,
            email VARCHAR,
            birth_date DATE,
            signup_date DATE,
            source_system VARCHAR,
            is_active BOOLEAN,
            deleted_date DATE,
            insert_timestamp TIMESTAMP,
            update_timestamp TIMESTAMP
        )
    """)


def ingest_silver(day: date = None, init: bool = False):
    """Relit les snapshots EN et FR depuis bronze/, les unifie et les upsert dans silver_passengers."""
    df_en = fetch_csv(*_snapshot_file("en", day, init))
    df_fr = fetch_csv(*_snapshot_file("fr", day, init))
    snapshot_date = date(2025, 8, 31) if init else day

    con = get_connection()
    create_silver_table(con)
    con.register("snapshot_en", df_en)
    con.register("snapshot_fr", df_fr)

    # Consolidation des deux sources dans le schéma unique. Côté FR : renommage des colonnes,
    # genre Homme/Femme -> Male/Female, dates DD/MM/YYYY -> DATE. Les passenger_id ne se
    # chevauchent pas entre les deux fichiers (cf. README du dataset), un UNION ALL suffit.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE snapshot AS
        SELECT
            passenger_id,
            first_name,
            last_name,
            gender,
            nationality,
            email,
            CAST(birth_date AS DATE) AS birth_date,
            CAST(signup_date AS DATE) AS signup_date,
            'EN' AS source_system,
            true AS is_active
        FROM snapshot_en
        UNION ALL
        SELECT
            id_passager,
            prenom,
            nom,
            CASE genre WHEN 'Homme' THEN 'Male' WHEN 'Femme' THEN 'Female' END,
            nationalite,
            email,
            CAST(strptime(date_naissance, '%d/%m/%Y') AS DATE),
            CAST(strptime(date_inscription, '%d/%m/%Y') AS DATE),
            'FR',
            true
        FROM snapshot_fr
    """)

    # Upsert par passenger_id (corrections d'email, nom, nationalité : même id).
    update_cols = PASSENGER_COLS + ["is_active"]
    set_clause = ", ".join(f"{c} = excluded.{c}" for c in update_cols)
    set_clause += ", deleted_date = NULL, update_timestamp = now()"

    con.execute(f"""
        INSERT INTO silver_passengers (
            passenger_id, {", ".join(update_cols)}, deleted_date, insert_timestamp, update_timestamp
        )
        SELECT passenger_id, {", ".join(update_cols)}, NULL, now(), now()
        FROM snapshot
        ON CONFLICT (passenger_id) DO UPDATE SET {set_clause}
    """)

    # Passager absent des DEUX fichiers du jour = supprimé côté source : désactivation.
    con.execute(
        """
        UPDATE silver_passengers SET is_active = false, deleted_date = ?, update_timestamp = now()
        WHERE is_active = true AND passenger_id NOT IN (SELECT passenger_id FROM snapshot)
        """,
        [snapshot_date],
    )

    con.unregister("snapshot_en")
    con.unregister("snapshot_fr")
    con.close()


def ingest_gold():
    # TODO : reconstruire la/les table(s) de gold avec les données passengers à partir de silver_passengers
    raise NotImplementedError


def init():
    ingest_bronze(init=True)
    ingest_silver(init=True)
    ingest_gold()
    print("Passagers (init) ingérés.")


if __name__ == "__main__":
    init()
