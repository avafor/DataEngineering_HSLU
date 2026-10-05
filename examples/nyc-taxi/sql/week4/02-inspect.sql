-- Run each query separately in pgAdmin and record its result.
SELECT COUNT(*) AS input_rows FROM public.taxi_trips_monthly;

SELECT COUNT(*) AS missing_passenger_count
FROM public.taxi_trips_monthly WHERE passenger_count IS NULL;

SELECT COUNT(*) AS negative_fares
FROM public.taxi_trips_monthly WHERE fare_amount_usd < 0;

SELECT COUNT(*) AS nonpositive_durations
FROM public.taxi_trips_monthly WHERE dropoff_time <= pickup_time;

SELECT * FROM public.taxi_trips_monthly
WHERE fare_amount_usd < 0 OR dropoff_time <= pickup_time
LIMIT 10;
