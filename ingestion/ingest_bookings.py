"""
À compléter : ingestion de l'entité `bookings`, découpée en 3 fonctions bronze/silver/gold
(même structure que `ingest_airports.py`, à utiliser comme modèle).
"""
from datetime import date

from common import fetch_csv, get_connection

BOOKING_COLS = [
    "passenger_id", "flight_id", "airport_id", "seat_class",
    "amount", "currency", "booking_date", "booking_channel",
]


def _daily_file(day: date = None, init: bool = False):
    if init:
        return "init", "bookings.csv"
    assert day is not None
    return "2025-09", f"bookings_{day.isoformat()}.csv"


def ingest_bronze(day: date = None, init: bool = False):
    """Télécharge le fichier bookings du jour (ou de init/) vers bronze/."""
    subdir, filename = _daily_file(day, init)
    fetch_csv(subdir, filename)


def create_silver_table(con):
    # Pas de is_active/deleted_date : une réservation est un évènement, elle ne disparaît pas.
    # amount en DECIMAL plutôt qu'en DOUBLE : c'est un montant monétaire (pas d'arrondi flottant).
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_bookings (
            booking_id VARCHAR PRIMARY KEY,
            passenger_id VARCHAR,
            flight_id VARCHAR,
            airport_id VARCHAR,
            seat_class VARCHAR,
            amount DECIMAL(10, 2),
            currency VARCHAR,
            booking_date DATE,
            booking_channel VARCHAR,
            insert_timestamp TIMESTAMP,
            update_timestamp TIMESTAMP
        )
    """)


def ingest_silver(day: date = None, init: bool = False):
    """Relit le fichier du jour depuis bronze/ et insère ses réservations dans silver_bookings."""
    df = fetch_csv(*_daily_file(day, init))

    con = get_connection()
    create_silver_table(con)
    con.register("daily", df)

    # Insert simple : le fichier ne contient que les réservations du jour, et une réservation ne
    # change jamais une fois créée. ON CONFLICT DO NOTHING rend le chargement idempotent :
    # rejouer un jour déjà chargé n'insère rien et ne modifie rien (pas même update_timestamp).
    con.execute(f"""
        INSERT INTO silver_bookings (
            booking_id, {", ".join(BOOKING_COLS)}, insert_timestamp, update_timestamp
        )
        SELECT
            booking_id, passenger_id, flight_id, airport_id, seat_class,
            CAST(amount AS DECIMAL(10, 2)), currency, CAST(booking_date AS DATE), booking_channel,
            now(), now()
        FROM daily
        ON CONFLICT (booking_id) DO NOTHING
    """)

    con.unregister("daily")
    con.close()


def ingest_gold():
    # TODO : reconstruire la/les table(s) de gold avec les données booking à partir de silver_bookings. 
    raise NotImplementedError


def init():
    ingest_bronze(init=True)
    ingest_silver(init=True)
    ingest_gold()
    print("Réservations (init) ingérés.")


if __name__ == "__main__":
    init()
