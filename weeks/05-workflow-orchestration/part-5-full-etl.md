# Week 5 — Part 5: Build a complete ETL flow

[Week overview](README.md) · [Previous: monthly ingestion](part-4-run-ingestion.md)

## Goal

Combine the three stages in one flow: extract a monthly file, transform selected columns, and load the result into a report table.

```text
TLC Parquet file → Python transformation → PostgreSQL taxi_trips_etl
     extract              transform/load
```

The earlier activities separated these ideas so that each was easier to inspect. This activity first shows a one-task ETL and then refactors it into three tasks.

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

The tasks must have clear boundaries. In the provided three-script flow, the tasks share the Week 5 `data` volume; the Parquet and CSV files therefore remain available after a task container ends:

1. **Extract** downloads the monthly Parquet file into the shared data directory.
2. **Transform** reads it, calculates `duration_minutes` and `trip_category`, and writes `transformed.csv`.
3. **Load** reads the CSV and inserts rows using the deterministic hash and `ON CONFLICT DO NOTHING`.

Use the task logs to show which stage completed. Deliberately make the load task fail after the transform succeeds. Inspect the execution, then rerun or restart from the failed stage after fixing the cause.

**Discuss:** Could the load task reuse the transformed output after a system interruption? What must be stored for that to work? Why does separating tasks improve observability, while still requiring a transaction and safe rerun design?

## Scheduling exercise

The flow currently starts only when someone clicks **Execute**. Add a monthly schedule so Kestra can start the flow automatically.

Add this trigger to the three-task flow and complete the missing properties:

```yaml
triggers:
  - id: monthly_schedule
    type: io.kestra.plugin.core.trigger.Schedule
    # Add a cron expression that runs on the first day of every month.
    cron: "..."
    # Pass the scheduled year and month to the flow inputs.
    inputs:
      year: "..."
      month: "..."
```

Use `trigger.date` to obtain the scheduled date. The flow should receive the year and month represented by that date, rather than always using the input defaults.

After saving the flow:

1. Check that the schedule is enabled in Kestra.
2. Inspect the trigger details and its next scheduled time.
3. Run the flow manually once and compare it with a scheduled execution.
4. Check the execution inputs to confirm which year and month the trigger supplied.

**Discuss:** Which month should a run on 1 March process? Why should the schedule use the scheduled date rather than the machine's current date? How would you process a month that was missed while the schedule was disabled?

### Other Kestra trigger types

Scheduling is only one way to start a flow. Kestra also supports:

- **Flow triggers:** start this flow when another Kestra flow reaches a selected state.
- **Webhook triggers:** start the flow when an HTTP request arrives.
- **Polling triggers:** check periodically whether new data is available.
- **File triggers:** start when a file appears in a configured location, such as cloud storage.
- **Realtime triggers:** start when an event arrives with low latency.
- **MCP Tool triggers:** allow an external AI tool to start the flow.

Kestra plugins add triggers for systems such as Kafka, SQS, databases, and cloud-storage services. The right trigger depends on how the source announces new data: a clock, an event, a new file, or the completion of another workflow.

## Retry exercise

Some failures are temporary. A network request may time out, or PostgreSQL may be unavailable for a short period. Add a retry policy to the Extract task:

```yaml
retry:
  type: constant
  interval: PT30S
  maxAttempts: 3
```

Kestra then makes at most three attempts. It waits 30 seconds between attempts.

Retries can help with temporary failures, such as a short network interruption. They do not fix permanent failures, such as an incorrect file name, missing Python package, invalid code, or wrong database password. Retrying those errors only repeats the same failure.

**Exercise:** Add the retry block to Extract. Temporarily make the first attempt fail, then allow the next attempt to succeed. Inspect the task logs and count the attempts. Afterwards, remove the deliberate failure.

## Restart exercise: continue from Load

Use [06-restart-three-scripts.yaml](flows/06-restart-three-scripts.yaml) for this exercise.

1. Run Extract and Transform successfully.
2. Load fails because `data/load_ready` does not exist.
3. Open the failed execution in Kestra. The execution page shows the three task states; Extract and Transform should be successful and Load should be failed.
4. Fix the external condition without editing the flow:

```sh
touch data/load_ready
```
5. Open the execution's **Actions** menu and choose **Restart**. Restart reruns the failed task in the same execution. If your Kestra version does not offer Restart, start a new execution after creating the marker file.
6. Compare the logs. Extract and Transform should remain successful; Load should run again.
7. Check the result in pgAdmin:

```sql
SELECT COUNT(*) AS loaded_rows
FROM public.taxi_trips_etl;
```

Run Load once more. The count should stay unchanged because the hash and `ON CONFLICT DO NOTHING` rule skip records already loaded.

After the exercise, remove the marker file:

```sh
rm data/load_ready
```

**Discuss:** What file or task output allowed Load to continue? What would be lost if the transformed file existed only inside a temporary task container?
