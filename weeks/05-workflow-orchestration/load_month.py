"""Load transformed trips using deterministic hashes to skip repeat records."""
import argparse
from datetime import date
import hashlib
import json
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import URL, MetaData, Table, create_engine, text
from sqlalchemy.dialects.postgresql import insert

# This is our teaching definition of a duplicate, not an official TLC trip ID.
HASH_COLUMNS = [
    "pickup_time", "dropoff_time", "trip_distance_miles",
    "pickup_zone_id", "dropoff_zone_id", "fare_amount_usd",
]


def make_hash(row):
    values = []
    for value in row:
        if pd.isna(value):
            values.append(None)
        elif isinstance(value, pd.Timestamp):
            values.append(value.isoformat())
        elif hasattr(value, "item"):
            values.append(value.item())
        else:
            values.append(value)
    encoded = json.dumps(values, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def load_file(csv_path, year, month, engine):
    source_month = date(year, month, 1)
    processed = inserted = 0
    schema_sql = (Path(__file__).parent / "sql/schema-etl.sql").read_text()
    with engine.begin() as connection:
        connection.execute(text("SET LOCAL lock_timeout = '10s'"))
        connection.execute(text(schema_sql))
        legacy_rows = connection.scalar(text(
            "SELECT EXISTS (SELECT 1 FROM public.taxi_trips_etl WHERE row_hash IS NULL)"
        ))
        if legacy_rows:
            raise ValueError("Existing ETL rows have no hash. Follow the legacy-table note in Part 5 before loading.")
        table = Table("taxi_trips_etl", MetaData(), autoload_with=connection, schema="public")
        # Fixed types keep hashes identical across batches with/without missing values.
        types = {"pickup_zone_id": "Int64", "dropoff_zone_id": "Int64",
                 "trip_distance_miles": "float64", "fare_amount_usd": "float64"}
        with pd.read_csv(csv_path, chunksize=1000, dtype=types, float_precision="round_trip") as batches:
            for trips in batches:
                for name in ("pickup_time", "dropoff_time"):
                    trips[name] = pd.to_datetime(trips[name], format="mixed")
                trips["source_month"] = source_month
                trips["row_hash"] = [make_hash(row) for row in trips[HASH_COLUMNS].itertuples(index=False, name=None)]
                # Convert pandas missing values (NaT/NaN) to SQL NULL.
                rows = trips.astype(object).where(pd.notna(trips), None).to_dict("records")
                # RETURNING lists only rows actually inserted, excluding conflicts.
                statement = insert(table).values(rows).on_conflict_do_nothing().returning(table.c.row_hash)
                result = connection.execute(statement)
                processed += len(rows)
                inserted += len(result.fetchall())
        if processed == 0:
            raise ValueError("The transformed file contains no trips; rolling back.")
    print(f"Committed {source_month:%Y-%m}: processed {processed:,}, inserted {inserted:,}, skipped {processed - inserted:,}")
    return inserted


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, choices=range(1, 13), required=True)
    args = parser.parse_args()
    engine = create_engine(URL.create(
        "postgresql+psycopg", host=os.environ.get("POSTGRES_HOST", "postgres"), port=5432,
        database=os.environ["POSTGRES_DB"], username=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    ), hide_parameters=True)
    try:
        load_file(Path("/app/data") / "transformed.csv", args.year, args.month, engine)
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
