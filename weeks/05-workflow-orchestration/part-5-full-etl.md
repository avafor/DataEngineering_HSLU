# Week 5 — Part 5: Build a complete ETL flow

[Week overview](README.md) · [Previous: monthly ingestion](part-4-run-ingestion.md)

## Goal

Combine the three stages in one flow: extract a monthly file, transform selected columns, and load the result into a report table.

```text
TLC Parquet file → Python transformation → PostgreSQL taxi_trips_etl
     extract              transform/load
```

The earlier activities separated these ideas so that each was easier to inspect. This final activity combines them into one repeatable task. The Python script uses one transaction, so a failed load does not leave a half-replaced month.

## 1. Read the ETL script

Open [`etl_month.py`](etl_month.py). Identify:

- where the monthly file is found or downloaded;
- where duration and `trip_category` are calculated;
- where columns are renamed;
- where the result is written to `public.taxi_trips_etl`.

The destination is a separate table. It does not alter `taxi_trips_monthly` or the SQL sample table.

## 2. Complete the flow

Open [05-full-etl-starter.yaml](flows/05-full-etl-starter.yaml), create a new flow named `taxi_full_etl`, and replace the placeholder commands with one command that runs:

```sh
python etl_month.py --year <selected year> --month <selected month>
```

Use the Kestra input expressions from the earlier flow. Rebuild the image first so it contains `etl_month.py`:

```sh
docker build -t deng-week5-ingest:local .
```

## 3. Execute and verify

Run year 2024, month 1. In pgAdmin, check:

```sql
SELECT source_month, trip_category, COUNT(*) AS rows_loaded,
       ROUND(AVG(duration_minutes)::numeric, 2) AS average_minutes
FROM public.taxi_trips_etl
GROUP BY source_month, trip_category
ORDER BY source_month, trip_category;
```

Rerun the same month. The row count should remain stable because the script replaces that month inside one transaction. Run February and compare the output groups.

**Discuss:** Which part is extraction, transformation and loading? What does Kestra coordinate, and what does the Python script perform? Which checks would you add before declaring the ETL successful?

**Finish with:** a successful execution, the destination query result, and a short explanation of the data flow and rerun behavior.

## Final challenge — split the ETL into three tasks

The current flow runs extraction, transformation and loading inside one Python task. Refactor it into three separate Kestra tasks:

```text
Extract → Transform → Load
```

The tasks must have clear boundaries:

1. **Extract** downloads or reuses the monthly Parquet file and saves it as a Kestra task output.
2. **Transform** reads the extract output, calculates `duration_minutes` and `trip_category`, and saves a transformed file as its output.
3. **Load** reads the transformed output and writes the selected month to `public.taxi_trips_etl` in one database transaction.

Use the task logs to show which stage completed. Deliberately make the load task fail after the transform succeeds. Inspect the execution, then rerun or restart from the failed stage after fixing the cause.

**Discuss:** Could the load task reuse the transformed output after a system interruption? What must be stored for that to work? Why does separating tasks improve observability, while still requiring a transaction and safe rerun design?


