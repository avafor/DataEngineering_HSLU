"""Create the transformed CSV used by the load task."""
import pandas as pd
import pyarrow.parquet as pq

source = pq.ParquetFile('/app/data/source.parquet')
frames = []
for batch in source.iter_batches(columns=['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'trip_distance', 'PULocationID', 'DOLocationID', 'fare_amount']):
    frame = batch.to_pandas()
    frame['duration_minutes'] = (frame['tpep_dropoff_datetime'] - frame['tpep_pickup_datetime']).dt.total_seconds() / 60
    frame['trip_category'] = frame['duration_minutes'].map(lambda value: 'unknown' if pd.isna(value) else 'invalid' if value < 0 else 'short' if value < 20 else 'long')
    frames.append(frame)
result = pd.concat(frames, ignore_index=True).rename(columns={'tpep_pickup_datetime': 'pickup_time', 'tpep_dropoff_datetime': 'dropoff_time', 'trip_distance': 'trip_distance_miles', 'PULocationID': 'pickup_zone_id', 'DOLocationID': 'dropoff_zone_id', 'fare_amount': 'fare_amount_usd'})
result.to_csv('/app/data/transformed.csv', index=False)
print(f'Transformed {len(result):,} rows')
