"""Download selected yellow taxi months and load every record in 10,000-row batches."""

import argparse
from datetime import date
import os
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory
from urllib.request import urlopen

import pyarrow.parquet as pq
from sqlalchemy import URL, create_engine, text


def download_month(year, month, directory):
    """Reuse a prepared file, or download it to this run's temporary directory."""
    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    prepared_file = Path("data") / filename
    if prepared_file.is_file():
        print(f"Using prepared file: {prepared_file}")
        return prepared_file

    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"
    path = Path(directory) / filename
    print(f"Downloading {url}")
    with urlopen(url, timeout=60) as response, path.open("wb") as output:
        shutil.copyfileobj(response, output, length=1024 * 1024)
    return path


def load_month(file_path, year, month, engine):
    columns = {
        "VendorID": "vendor_id",
        "tpep_pickup_datetime": "pickup_time",
        "tpep_dropoff_datetime": "dropoff_time",
        "passenger_count": "passenger_count",
        "trip_distance": "trip_distance_miles",
        "PULocationID": "pickup_zone_id",
        "DOLocationID": "dropoff_zone_id",
        "fare_amount": "fare_amount_usd",
        "total_amount": "total_amount_usd",
    }
    source_month = date(year, month, 1)
    schema = (Path(__file__).parent / "sql/schema-monthly.sql").read_text()
    with pq.ParquetFile(file_path) as source:
        expected = source.metadata.num_rows
        if expected == 0 or not set(columns).issubset(source.schema_arrow.names):
            raise ValueError("The source is empty or lacks required yellow taxi columns.")

        # Commit one whole month at a time. A failed month restores its earlier rows.
        with engine.begin() as connection:
            # In this transaction, wait at most 10 seconds to acquire a database lock.
            # This limits lock waiting, not the total ingestion time.
            connection.execute(text("SET LOCAL lock_timeout = '10s'"))
            connection.execute(text(schema))
            # Prevent two simultaneous jobs from replacing the same month together.
            connection.execute(text("LOCK TABLE public.taxi_trips_monthly IN SHARE ROW EXCLUSIVE MODE"))
            connection.execute(text(
                "DELETE FROM public.taxi_trips_monthly WHERE source_month = :month"
            ), {"month": source_month})
            loaded = 0

            # Unlike next(...), this loop visits ALL batches in the file.
            for batch in source.iter_batches(batch_size=10_000, columns=list(columns)):
                trips = batch.to_pandas().rename(columns=columns)
                for name in ("vendor_id", "passenger_count", "pickup_zone_id", "dropoff_zone_id"):
                    trips[name] = trips[name].astype("Int64")
                trips["source_month"] = source_month
                trips.to_sql("taxi_trips_monthly", connection, schema="public",
                             if_exists="append", index=False, chunksize=1_000)
                loaded += len(trips)
                print(f"{source_month:%Y-%m}: wrote {loaded:,}/{expected:,} rows (not committed yet)")

            actual = connection.scalar(text(
                "SELECT COUNT(*) FROM public.taxi_trips_monthly WHERE source_month = :month"
            ), {"month": source_month})
            if loaded != expected or actual != expected:
                raise ValueError("Loaded count does not match the source file; rolling back this month.")
    print(f"Committed {source_month:%Y-%m}: {loaded:,} rows")
    return loaded


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, required=True, help="Publication year, e.g. 2024")
    parser.add_argument("--months", type=int, choices=range(1, 13), nargs="+", required=True,
                        help="Months to load, e.g. 1 2 3")
    args = parser.parse_args()
    if not 2009 <= args.year <= date.today().year:
        parser.error("Choose a year from 2009 through the current year.")

    url = URL.create(
        "postgresql+psycopg", host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=5432, database=os.environ["POSTGRES_DB"],
        username=os.environ["POSTGRES_USER"], password=os.environ["POSTGRES_PASSWORD"],
    )
    engine = create_engine(url, connect_args={"connect_timeout": 10}, hide_parameters=True)
    try:
        # Check credentials before downloading potentially large files.
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        for month in sorted(set(args.months)):
            with TemporaryDirectory() as directory:
                path = download_month(args.year, month, directory)
                load_month(path, args.year, month, engine)
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
