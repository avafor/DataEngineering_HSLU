-- Flink SQL: read rider GPS pings from Redpanda and aggregate them per rider and minute.
--
-- Fill the topic first, e.g. from a terminal in the repository root:
--   uv run --project streaming weeks/09-event-streaming/examples/producer.py
-- (the exercise notebooks 01-05 write to the same topic with the same fields).
--
-- Usage:
--   1. Open Dinky (port 8888 in PORTS, or http://localhost:8888 locally)
--   2. Create a new FlinkSQL task (Cluster Configuration: deng, Catalog: DefaultCatalog)
--   3. Paste this script and run it
--
-- Or run it from the Flink SQL client inside the jobmanager container:
--   docker exec -it flink-jobmanager ./bin/sql-client.sh
--   then paste the statements below.

CREATE TABLE IF NOT EXISTS rider_locations (
    rider_id     STRING,
    lat          DOUBLE,
    lon          DOUBLE,
    speed_kmh    DOUBLE,
    `timestamp`  DOUBLE,   -- UNIX seconds, as written by the producers
    event_time AS TO_TIMESTAMP_LTZ(CAST(`timestamp` * 1000 AS BIGINT), 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'rider-locations',
    'properties.bootstrap.servers' = 'redpanda:29092',
    'properties.group.id' = 'flink-rider-locations',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- Tumbling window: pings, average and top speed per rider per minute.
-- Speeds above 80 km/h are GPS glitches (see exercise 05/06) and are left out.
SELECT
    window_start,
    window_end,
    rider_id,
    COUNT(*)                 AS pings,
    ROUND(AVG(speed_kmh), 1) AS avg_speed_kmh,
    MAX(speed_kmh)           AS max_speed_kmh
FROM TABLE(
    TUMBLE(TABLE rider_locations, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
)
WHERE speed_kmh <= 80
GROUP BY window_start, window_end, rider_id;
