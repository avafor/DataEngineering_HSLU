"""Transform a monthly Parquet file in batches, keeping memory use bounded."""
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

COLUMNS = {
    "tpep_pickup_datetime": "pickup_time",
    "tpep_dropoff_datetime": "dropoff_time",
    "trip_distance": "trip_distance_miles",
    "PULocationID": "pickup_zone_id",
    "DOLocationID": "dropoff_zone_id",
    "fare_amount": "fare_amount_usd",
}


def transform_file(source_path, output_path):
    count = 0
    with pq.ParquetFile(source_path) as source:
        if source.metadata.num_rows == 0:
            raise ValueError("The source file contains no trips.")
        for batch in source.iter_batches(batch_size=10_000, columns=list(COLUMNS)):
            trips = batch.to_pandas().rename(columns=COLUMNS)
            trips["duration_minutes"] = (
                trips["dropoff_time"] - trips["pickup_time"]
            ).dt.total_seconds() / 60
            trips["trip_category"] = "long"
            trips.loc[trips.duration_minutes < 20, "trip_category"] = "short"
            trips.loc[trips.duration_minutes < 0, "trip_category"] = "invalid"
            trips.loc[trips.duration_minutes.isna(), "trip_category"] = "unknown"
            # Write the header once; append subsequent batches.
            trips.to_csv(output_path, index=False, mode="w" if count == 0 else "a",
                         header=count == 0)
            count += len(trips)
    print(f"Transformed {count:,} rows into {output_path}")


def main():
    transform_file(Path("/app/data") / "source.parquet", Path("/app/data") / "transformed.csv")


if __name__ == "__main__":
    main()
