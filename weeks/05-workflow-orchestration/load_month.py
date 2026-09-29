"""Load the transformed CSV into PostgreSQL."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import pandas as pd
from sqlalchemy import URL, MetaData, Table, create_engine, text
from sqlalchemy.dialects.postgresql import insert

parser = argparse.ArgumentParser()
parser.add_argument('--year', type=int, required=True)
parser.add_argument('--month', type=int, choices=range(1, 13), required=True)
args = parser.parse_args()
month = f'{args.year}-{args.month:02d}-01'
frame = pd.read_csv('/app/data/transformed.csv')
frame['source_month'] = pd.Timestamp(month).date()
frame['pickup_time'] = pd.to_datetime(frame['pickup_time'])
frame['dropoff_time'] = pd.to_datetime(frame['dropoff_time'])

# These fields define when two input rows are considered the same record.
hash_columns = [
    'pickup_time', 'dropoff_time', 'trip_distance_miles',
    'pickup_zone_id', 'dropoff_zone_id', 'fare_amount_usd',
]


def make_hash(row):
    values = []
    for value in row:
        if pd.isna(value):
            values.append(None)
        elif isinstance(value, pd.Timestamp):
            values.append(value.isoformat())
        elif hasattr(value, 'item'):
            values.append(value.item())
        else:
            values.append(value)
    encoded = json.dumps(values, separators=(',', ':'), allow_nan=False)
    return hashlib.sha256(encoded.encode('utf-8')).hexdigest()


frame['row_hash'] = [make_hash(row) for row in frame[hash_columns].itertuples(index=False, name=None)]
columns = ['row_hash', 'source_month', 'pickup_time', 'dropoff_time', 'trip_distance_miles', 'pickup_zone_id', 'dropoff_zone_id', 'fare_amount_usd', 'duration_minutes', 'trip_category']
engine = create_engine(URL.create('postgresql+psycopg', host='postgres', port=5432, database=os.environ['POSTGRES_DB'], username=os.environ['POSTGRES_USER'], password=os.environ['POSTGRES_PASSWORD']))
with engine.begin() as connection:
    schema_sql = Path('/app/sql/schema-etl.sql').read_text()
    connection.execute(text(schema_sql))

    # Read the table definition so SQLAlchemy knows its columns and types.
    table = Table(
        'taxi_trips_etl', MetaData(), autoload_with=connection, schema='public'
    )

    # Insert in small groups; an existing hash is skipped.
    for start in range(0, len(frame), 1000):
        rows = frame[columns].iloc[start:start + 1000].to_dict('records')
        statement = insert(table).values(rows)
        statement = statement.on_conflict_do_nothing()
        connection.execute(statement)
print(f'Loaded {len(frame):,} rows for {month[:7]}')
