"""Validate the loaded result for one taxi month."""

import argparse
from datetime import date
import os

from sqlalchemy import URL, create_engine, text


def validate_month(year, month, engine):
    source_month = date(year, month, 1)

    with engine.connect() as connection:
        row_count = connection.scalar(text("""
            SELECT COUNT(*)
            FROM public.taxi_trips_etl
            WHERE source_month = :month
        """), {"month": source_month})

        unexpected_categories = connection.scalar(text("""
            SELECT COUNT(*)
            FROM public.taxi_trips_etl
            WHERE source_month = :month
              AND trip_category NOT IN ('short', 'long', 'invalid', 'unknown')
        """), {"month": source_month})

    if row_count == 0:
        raise ValueError(f"Validation failed: no rows loaded for {source_month:%Y-%m}")

    if unexpected_categories > 0:
        raise ValueError("Validation failed: unexpected trip categories found")

    print(f"Validation passed: {row_count:,} rows for {source_month:%Y-%m}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, choices=range(1, 13), required=True)
    args = parser.parse_args()

    engine = create_engine(URL.create(
        "postgresql+psycopg",
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=5432,
        database=os.environ["POSTGRES_DB"],
        username=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    ))

    try:
        validate_month(args.year, args.month, engine)
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
