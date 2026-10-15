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

Use the task logs to show which stage completed. The restart exercise later in this file introduces a controlled Load failure.

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
```

Do not place `trigger.date` inside the trigger's `inputs` block. In this Kestra version, that expression is evaluated before the scheduled execution context is available. Instead, use it in every task command that needs the target year and month:

```yaml
--year {{ trigger.date is defined ? (trigger.date | date("yyyy")) : inputs.year }}
--month {{ trigger.date is defined ? (trigger.date | date("M")) : inputs.month }}
```

For a scheduled or backfill execution, the date-dependent tasks use the year and month from `trigger.date`. For a manual execution, `trigger.date` is unavailable, so the tasks use the values selected in the execution form. Check, Extract, Load and Validate must all use the same rule.

After saving the flow:

1. Check that the schedule is enabled in Kestra.
2. Inspect the trigger details and its next scheduled time.
3. Run the flow manually once and compare it with a scheduled execution.
4. Check the Extract and Load command logs to confirm which year and month the tasks used.

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

## Source-availability check

Before adding another task, consider these questions:

- What happens if the selected TLC file has not been published yet?
- Should the pipeline begin its transformations before it knows that the source exists?
- What error would a student or operator see if the download URL returned `404 Not Found`?
- Which task should fail: Extract, Transform or Load?
- Would retrying help if the server were temporarily unavailable? Would it help if the file name were wrong?

One solution is to check the source before downloading the complete file:

```text
Check source → Extract → Transform → Load
```

Open [`check_source.py`](check_source.py). The `check_source()` function constructs the monthly TLC URL and sends a small request to verify that the file is available.

The script is part of the ingestion image. Rebuild the image once before running the four-task flow:

```sh
docker build -t deng-week5-ingest:local .
```

PostgreSQL, pgAdmin and Kestra do not need to be restarted after this build.

Open [08-four-task-etl.yaml](flows/08-four-task-etl.yaml) and identify where it calls:

```sh
python /app/check_source.py --year <year> --month <month>
```

Run the flow once for a published month and once for a future or unavailable month. Compare the `check_source` logs. Confirm that Extract does not start when the check fails.

**Discuss:** Is checking first a guarantee that the later download will succeed? What could change between the check and the download?

## Post-load validation

A successful Load task only proves that the database accepted the commands. It does not prove that the expected data was loaded.

Before looking at the solution, discuss:

- What is the simplest evidence that the selected month was loaded?
- Which transformed values should be allowed in `trip_category`?
- Should a failed validation remove the rows that Load already committed?
- Who should investigate when Load succeeds but validation fails?

Open [`validate_month.py`](validate_month.py). It checks that the selected month contains at least one row and that every trip category is expected.

The separate [09-five-task-etl.yaml](flows/09-five-task-etl.yaml) adds this final stage:

```text
Check source → Extract → Transform → Load → Validate
```

Rebuild the image because it now contains `validate_month.py`:

```sh
docker build -t deng-week5-ingest:local .
```

Run the flow for an available month and inspect the Validate logs. Then run `validate_month.py` for a month that was not loaded and explain why the task fails.

**Discuss:** Why should a failed validation fail the task instead of only printing a warning?

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

Open [07-retry-exercise.yaml](flows/07-retry-exercise.yaml). Your goal is to make attempt 1 fail and a later attempt succeed without editing the flow between attempts.

Work in pairs and design a small temporary condition. It must persist outside the short-lived task container so that the next attempt can observe that the situation changed. One possible category is a marker file in the shared `data` directory, but devise the exact logic yourselves.

Complete the flow:

1. Choose a retry type, interval and maximum number of attempts.
2. Write the command that detects your temporary condition.
3. Make the first attempt exit with a non-zero status.
4. Make a later attempt exit successfully.
5. Add log messages that make the two outcomes easy to distinguish.
6. Run the flow and compare **Attempt 1** with the successful attempt in Kestra.

Before another run, reset whatever external state your solution created.

**Discuss:** Why is a retry appropriate for a temporary network problem but not for invalid Python code or a wrong password? What could happen if retries repeatedly execute a task that is not safe to rerun?

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
