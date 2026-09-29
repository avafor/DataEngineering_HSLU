-- Replace the two placeholder expressions below.
-- Show duration in minutes and classify trips using a threshold of your choice.
SELECT pickup_time,
       dropoff_time,
       trip_distance_miles,
       NULL::numeric AS duration_minutes,       -- TODO: calculate duration
       'not classified'::text AS trip_category  -- TODO: write a CASE expression
FROM public.taxi_trip_sample
WHERE source_month = DATE '2024-01-01'
ORDER BY pickup_time, dropoff_time, trip_distance_miles
LIMIT 10;
