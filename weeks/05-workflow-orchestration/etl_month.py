"""Extract one taxi month, transform selected fields, and load a report table."""

import argparse
import os
from pathlib import Path
from tempfile import TemporaryDirectory

from sqlalchemy import URL, create_engine

from ingest_months import download_month
from transform_month import transform_file
from load_month import load_file


def run(file_path, year, month, engine):
    # The one-task and three-task flows use exactly the same transformations
    # and hash-based load. Only Kestra's task boundaries differ.
    with TemporaryDirectory() as directory:
        transformed = Path(directory) / "transformed.csv"
        transform_file(file_path, transformed)
        load_file(transformed, year, month, engine)


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
