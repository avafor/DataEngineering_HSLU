"""Extract one taxi month, transform selected fields, and load a report table."""

import argparse
from datetime import date
import os
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import URL, create_engine, text

from ingest_months import download_month


def run(file_path, year, month, engine):
    source_month = date(year, month, 1)
    with pq.ParquetFile(file_path) as source, engine.begin() as connection:
        connection.execute(text((Path(__file__).parent / "sql/schema-etl.sql").read_text()))
        connection.execute(text("DELETE FROM public.taxi_trips_etl WHERE source_month = :month"), {"month": source_month})
        for batch in source.iter_batches(batch_size=10_000, columns=[
            "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance",
            "PULocationID", "DOLocationID", "fare_amount",
        ]):
            trips = batch.to_pandas()
            trips["duration_minutes"] = (
                trips["tpep_dropoff_datetime"] - trips["tpep_pickup_datetime"]
            ).dt.total_seconds() / 60
            trips["trip_category"] = trips["duration_minutes"].map(
                lambda value: "unknown" if pd.isna(value) else
                "invalid" if value < 0 else "short" if value < 20 else "long"
            )
            trips["source_month"] = source_month
            trips = trips.rename(columns={
                "tpep_pickup_datetime": "pickup_time",
                "tpep_dropoff_datetime": "dropoff_time",
                "trip_distance": "trip_distance_miles",
                "PULocationID": "pickup_zone_id",
                "DOLocationID": "dropoff_zone_id",
                "fare_amount": "fare_amount_usd",
            })
            trips.to_sql("taxi_trips_etl", connection, schema="public", if_exists="append", index=False, chunksize=1_000)
        count = connection.scalar(text("SELECT COUNT(*) FROM public.taxi_trips_etl WHERE source_month = :month"), {"month": source_month})
    print(f"ETL committed {source_month:%Y-%m}: {count:,} transformed rows")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, choices=range(1, 13), required=True)
    args = parser.parse_args()
    engine = create_engine(URL.create("postgresql+psycopg", host=os.environ.get("POSTGRES_HOST", "postgres"), port=5432,
        database=os.environ["POSTGRES_DB"], username=os.environ["POSTGRES_USER"], password=os.environ["POSTGRES_PASSWORD"]))
    try:
        with TemporaryDirectory() as directory:
            run(download_month(args.year, args.month, directory), args.year, args.month, engine)
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
