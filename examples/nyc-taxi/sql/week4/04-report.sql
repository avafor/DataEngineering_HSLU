-- Week 4 report exercise starter.
-- Replace every TODO placeholder before executing the CREATE VIEW statement.
CREATE OR REPLACE VIEW public.daily_zone_report AS
SELECT
    pickup_date,
    pickup_zone_id,
    pickup_zone_name,
    TODO_COUNT AS trip_count,
    TODO_FARE_TOTAL AS fare_total_usd
FROM TODO_SOURCE
WHERE TODO_FILTER
GROUP BY TODO_GROUPS;

SELECT * FROM public.daily_zone_report
ORDER BY pickup_date, fare_total_usd DESC;

-- Verification: these counts must match.
SELECT COUNT(*) AS included_records
FROM public.trips_reviewed WHERE report_status = 'included';

SELECT COALESCE(SUM(trip_count), 0) AS reported_records
FROM public.daily_zone_report;
