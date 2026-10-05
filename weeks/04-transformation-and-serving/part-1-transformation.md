# Week 4 — Part 1: From taxi records to a daily report

[Week 4 overview](README.md) · [Next: tokenization](part-2-tokenization.md)

## Goal and starting point

Create a report of trip counts and fare totals per pickup date and zone using the records already in `public.taxi_trips_monthly`. We will preserve those records and create SQL views that derive fields, flag records, and calculate totals.

Use the Week 2 database `ny_taxi` in pgAdmin. Open its Query Tool, as in [Week 2 Part 2](../02-postgresql-and-ingestion/part-2-postgres-and-pgadmin.md). Run:

```sql
SELECT COUNT(*) AS input_rows FROM public.taxi_trips_monthly;
```

Record the count. If the table is missing or empty, complete [Week 2 Part 3, Step 7](../02-postgresql-and-ingestion/part-3-python-ingestion.md#step-7--download-and-load-several-complete-months). One complete month is sufficient for this exercise; you can also use all 12 months of 2024. The monthly loader replaces each requested month on rerun.

Wait for ingestion to finish before comparing counts: each month's rows become visible only after that month commits. Run this query to see which source months are available:

```sql
SELECT source_month, COUNT(*) AS loaded_rows
FROM public.taxi_trips_monthly
GROUP BY source_month
ORDER BY source_month;
```

The queries in this exercise use the monthly data you loaded into `public.taxi_trips_monthly`. Wait until ingestion finishes before continuing so that your results stay consistent while you work. Queries over a full year may take longer because they process more records.

We will create two views in the `public` schema: `trips_reviewed`, which adds calculated fields and reporting flags, and `daily_zone_report`, which summarizes the selected trips by day and pickup zone.

All terminal commands below run from `examples/nyc-taxi`. If the services are stopped:

```sh
docker compose up -d --wait
```

## Step 1 — Inspect before deciding what to change

Open [02-inspect.sql](../../examples/nyc-taxi/sql/week4/02-inspect.sql) in your editor. Copy each query into pgAdmin and execute it separately. `WHERE` keeps rows meeting a condition; `IS NULL` finds missing values.

Record the number of missing passenger counts, negative fares, and trips whose drop-off is at or before pickup. The last condition also finds zero-duration trips. Read a few matching records. A count of zero is a valid result; your loaded data may not contain every issue.

**Discuss:** Is a missing passenger count the same as zero passengers? Could a negative fare represent an adjustment? What additional information would help you decide?

**Finish with:** your input count and three quality counts. No records have been changed.

## Step 2 — Load the zone names

The trip table contains numeric zone identifiers. A **lookup table** maps each identifier to a name and borough. You have already downloaded `taxi_zone_lookup.csv` into `examples/nyc-taxi/data/` as part of the [Week 4 preparation](../../preparation/week-04.md). This file supplies that mapping.

Run:

```sh
docker compose run --rm ingest load_zones.py data/taxi_zone_lookup.csv
```

Read the command from left to right:

- `docker compose run` creates a container to run a command.
- `--rm` removes that container after the command finishes. It does **not** delete or replace a Python script.
- `ingest` selects the service configuration from `compose.yaml`, including its Python image and database connection settings.
- `load_zones.py` selects the script to run instead of the default `ingest.py`, **for this run only**.
- `data/taxi_zone_lookup.csv` is the file path passed to the script inside the container.

The Dockerfile defines `ENTRYPOINT ["python"]` and `CMD ["ingest.py"]`. Providing `load_zones.py data/taxi_zone_lookup.csv` overrides that default command, so Python runs:

```sh
python load_zones.py data/taxi_zone_lookup.csv
```

Both Python scripts remain in the image. The script reads the local CSV, creates `public.taxi_zones`, and loads the mapping. It does not download anything. After it finishes, `--rm` removes the temporary container, but the loaded rows remain in PostgreSQL.

If Python says the script does not exist, rebuild the image with `docker compose build ingest`.

In pgAdmin, run:

```sql
SELECT * FROM public.taxi_zones ORDER BY location_id LIMIT 10;
```

`public` is the same **schema** (a namespace for database objects) that contains our monthly trips table. `location_id` is the lookup table's primary key: there can be only one mapping per identifier. Refresh the Schemas entry in pgAdmin if you want to browse the new objects.

**Investigate:** Does running the zone-loading command several times add duplicate zones?

1. Predict what will happen to the number of rows.
2. Run this query in pgAdmin and note the result:

   ```sql
   SELECT COUNT(*) AS zone_count FROM public.taxi_zones;
   ```

3. Run the zone-loading command again, then rerun the count query. Does the result match your prediction?
4. Read [load_zones.py](../../examples/nyc-taxi/load_zones.py). Find the statements that explain the result. Is this behavior caused by `--rm` or by the script?

**Finish with:** a lookup table you can query, an explanation of what happens when you load it again, and why two names for the same identifier would cause trouble during a join.

## Step 3 — Derive fields, enrich, and flag records

Read [03-transform.sql](../../examples/nyc-taxi/sql/week4/03-transform.sql). Before executing it, use this guide:

| SQL expression | What it does here |
|---|---|
| `CREATE OR REPLACE VIEW` | Saves a query under a name; rerunning updates its definition. |
| `t` and `z` | Short aliases for the trip and zone tables. For example, `t.pickup_time` selects the trip table's pickup time. |
| `CAST(pickup_time AS DATE)` | Returns the date part of the pickup timestamp. |
| `EXTRACT(EPOCH FROM (...)) / 60.0` | Converts the difference between timestamps into minutes. |
| `LEFT JOIN ... ON ...` | Looks up the pickup-zone identifier; keeps the trip even if no mapping exists. |
| `CASE ... WHEN ... ELSE ... END` | Assigns a reporting status using the first matching condition. |

Our teaching report applies these rules in the listed order:

| Condition | Status | Treatment in report |
|---|---|---|
| Missing pickup time, drop-off time, or fare | `missing_required_value` | Exclude from this report; retain for inspection. |
| Drop-off is at or before pickup | `nonpositive_duration` | Exclude from this report; retain for inspection. |
| Fare is negative | `negative_fare` | Exclude from this report; investigate separately. |
| Pickup identifier has no lookup row | `unmatched_pickup_zone` | Exclude from this report; investigate the mapping. |
| None of the above | `included` | Include in this report. |

Missing passenger counts remain `NULL` and do not exclude a trip: this report does not require passenger counts. A lookup entry labelled unknown is still a matched entry; matching is not proof of a precise location. These are explicit exercise rules, not a complete definition of a trustworthy trip.

Copy and execute the SQL file in pgAdmin. Then inspect the view:

```sql
SELECT pickup_time, pickup_date, duration_minutes,
       pickup_zone_name, passenger_count, report_status
FROM public.trips_reviewed
LIMIT 20;
```

Compare its count with your original input count:

```sql
SELECT COUNT(*) AS reviewed_rows FROM public.trips_reviewed;
```

The counts must match. The left join retains unmatched trips, and the unique lookup key prevents a trip from matching several lookup rows.

**Discuss:** Why might an inner join hide a data-quality problem? If one record has two issues, why does our status show only one? The separate inspection queries can count overlapping issues; `CASE` gives each record one status.

**Suggest a solution:** Our current `CASE` reports only the first matching issue. How could you change the query so that a trip with both a negative fare and an unmatched pickup zone shows both issues? Propose one solution.

**Finish with:** a queryable view with dates, durations, zone names, and reporting statuses. The original table is unchanged.

## Step 4 — Build a report for a consumer

Our consumer is an analyst exploring historical trips in the loaded dataset. The requested output is one row per **pickup date and pickup zone**, containing a trip count and the sum of included fare amounts.

Read [04-report.sql](../../examples/nyc-taxi/sql/week4/04-report.sql):

- `WHERE report_status = 'included'` applies our reporting rules.
- `GROUP BY` gathers records with the same date and zone.
- `COUNT(*)` counts records in each group; `SUM` adds their fares.
- `CAST(... AS NUMERIC)` performs the aggregation using decimal arithmetic; it cannot recover precision already lost in the stored values.
- `ROUND(..., 2)` presents the total with two decimal places.
- `AS` labels a result column, such as `fare_total_usd`.

Run the file's statements individually. The last two queries must return the same count: every included record belongs to exactly one report group. `COALESCE(..., 0)` shows zero if the report is empty instead of a missing sum.

The metric is **the sum of nonnegative fare amounts for records meeting our rules**. It is not total revenue, profit, or the total charged including all extras. Its date comes from pickup time, not from the source-file month.

**Finish with:** a daily report, matching included/reported counts, and a sentence explaining what the total measures and what it leaves out.

## Step 5 — Change a rule and explain its effect

Predict what happens if negative fares are included. In `03-transform.sql`, remove the `WHEN t.fare_amount_usd < 0 ...` line from the view definition and execute the changed definition. Query the report again.

The existing report view uses the updated reviewed view immediately; you do not need to reload the taxi file. Counts and totals may change, depending on the data and the other rules. Restore the original definition afterward.

**Discuss:**

1. How does including negative fares change what the fare total measures? Would you use this total to answer the same business question as the total that excludes negative fares?
2. We kept the original trips and excluded negative fares only through the view. Why does this let us compare the two versions of the report? What would we need to do if we had deleted those trips during ingestion?

## If something fails

- **Missing `public.taxi_zones`:** complete Step 2 before creating the views.
- **Missing local CSV:** check `examples/nyc-taxi/data/taxi_zone_lookup.csv` and complete the preparation checklist.
- **Unexpected totals:** check the loaded source months, reporting rules, and whether ingestion is still running. Rerun the view definitions if they previously used the small table.
- **No report rows:** count the statuses in `public.trips_reviewed`; inspect the excluded records and the lookup table.
- **Values look different from a partner's:** compare input counts and loaded files before comparing transformations.

Continue to [Part 2 — Tokenization and access](part-2-tokenization.md).
