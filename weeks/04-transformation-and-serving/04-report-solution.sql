-- Instructor solution for Week 4, Part 1, Step 4.
-- Keep this file local until the solution should be shared.

CREATE OR REPLACE VIEW public.daily_zone_report AS
SELECT
    pickup_date,
    pickup_zone_id,
    pickup_zone_name,
    COUNT(*) AS trip_count,
    ROUND(SUM(CAST(fare_amount_usd AS NUMERIC)), 2) AS fare_total_usd
FROM public.trips_reviewed
WHERE report_status = 'included'
GROUP BY pickup_date, pickup_zone_id, pickup_zone_name;

SELECT * FROM public.daily_zone_report
ORDER BY pickup_date, fare_total_usd DESC;

-- These counts must match.
SELECT COUNT(*) AS included_records
FROM public.trips_reviewed
WHERE report_status = 'included';

SELECT COALESCE(SUM(trip_count), 0) AS reported_records
FROM public.daily_zone_report;
