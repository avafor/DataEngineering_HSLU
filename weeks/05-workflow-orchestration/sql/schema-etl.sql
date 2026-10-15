CREATE TABLE IF NOT EXISTS public.taxi_trips_etl (
    row_hash TEXT,
    source_month DATE NOT NULL,
    pickup_time TIMESTAMP WITHOUT TIME ZONE,
    dropoff_time TIMESTAMP WITHOUT TIME ZONE,
    trip_distance_miles DOUBLE PRECISION,
    pickup_zone_id INTEGER,
    dropoff_zone_id INTEGER,
    fare_amount_usd DOUBLE PRECISION,
    duration_minutes DOUBLE PRECISION,
    trip_category TEXT NOT NULL
);

ALTER TABLE public.taxi_trips_etl
    ADD COLUMN IF NOT EXISTS row_hash TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS taxi_trips_etl_row_hash_key
    ON public.taxi_trips_etl (row_hash)
    WHERE row_hash IS NOT NULL;
