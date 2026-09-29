-- Separate from the small taxi_trips table used earlier in the exercise.
CREATE TABLE IF NOT EXISTS public.taxi_trips_monthly (
    source_month DATE NOT NULL,
    vendor_id INTEGER,
    pickup_time TIMESTAMP WITHOUT TIME ZONE,
    dropoff_time TIMESTAMP WITHOUT TIME ZONE,
    passenger_count INTEGER,
    trip_distance_miles DOUBLE PRECISION,
    pickup_zone_id INTEGER,
    dropoff_zone_id INTEGER,
    fare_amount_usd DOUBLE PRECISION,
    total_amount_usd DOUBLE PRECISION
);
