# Week 11 — Part 6: Load and query taxi data in BigQuery

[Week 11 overview](README.md) · [Previous: upload to Cloud Storage](part-5-kestra-to-cloud-storage.md)

## Goal

Extend the two-task flow from Part 5 with a third task that loads the Parquet file into BigQuery. Inspect the table, query its rows, and investigate what happens when the flow runs again.

This is an optional follow-up to the core Terraform session. **BigQuery is the database that stores the analytical table and executes SQL. Kestra submits the load job and waits for its result.**

```mermaid
flowchart LR
    S["TLC file"] --> D["Kestra: download_taxi"]
    D --> U["Kestra: upload_to_gcs"]
    U --> G["Cloud Storage: Parquet object"]
    G --> L["Kestra submits a BigQuery load job"]
    L --> B["BigQuery: monthly taxi table"]
    B --> Q["SQL query in the BigQuery Console"]
```

Loading creates a regular BigQuery table containing the records; it is not an external table that reads the Parquet file for each query. The original object remains in Cloud Storage. Business transformations come later.

## 1. Continue from Part 5

You need:

- A working two-task flow from Part 5 and the same Kestra container with its pipeline key mounted.
- Your Terraform-created bucket and `taxi_analytics` dataset in the same course project.
- Both resources in **EU**, as configured in Part 3. The load job must use that dataset location.

If you already cleaned up, recreate the resources with the original Terraform configuration and state. If you deleted the pipeline key, create a replacement for `kestra-week11`, update its path, and recreate Kestra using both Compose files as explained in Part 5. Do not switch the host's Terraform credentials to the pipeline key.

Record the **project ID**, **bucket name**, **dataset ID**, and **dataset location**. A dataset groups tables; it does not yet contain taxi records merely because Terraform created it.

## 2. Add the pipeline's BigQuery permissions

Keep the existing `kestra-week11` service account and JSON key. Changing permissions does not require another key.

For this dedicated classroom project, use project-level grants as in Part 5:

1. Open **IAM & Admin → IAM** in your course project.
2. Find the `kestra-week11` service account and edit its roles.
3. Keep **Storage Object User**, then add the following roles and save:

| Additional role | Why it is needed |
|---|---|
| **BigQuery Job User** (`roles/bigquery.jobUser`) | Submit BigQuery jobs in this project. |
| **BigQuery Data Editor** (`roles/bigquery.dataEditor`) | Create and update the destination tables. At project level this applies across the project's datasets. |
| **Storage Bucket Viewer** (`roles/storage.bucketViewer`) | Read bucket metadata needed for the load, including `storage.buckets.get`. Part 5's object role provides access to file contents but does not include this bucket permission. |

The project-level Data Editor role is a classroom simplification. It can instead be granted on just `taxi_analytics`; Job User must still be granted on the project. We do not need to give Kestra BigQuery Admin or reuse the Terraform service account.

Your **student account** runs the SQL checks in the Console, while **Kestra's service account** runs the pipeline. If your student account lacks access, ask the instructor to check its query and table-read permissions separately.

References: [BigQuery load requirements](https://docs.cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet) · [Cloud Storage roles](https://docs.cloud.google.com/storage/docs/access-control/iam-roles).

## 3. Create a new flow from the starter

Open [02-taxi-to-bigquery-starter.yaml](flows/02-taxi-to-bigquery-starter.yaml). Create a **new flow** in Kestra and paste the entire file into the editor. It uses `id: taxi_to_bigquery` and `namespace: deng.week11`, so your Part 5 flow remains available.

The download and upload tasks are complete. Your task is to finish the commented `load_bigquery` task. **Do not execute the incomplete starter expecting a BigQuery table:** until you enable the third task, success means only download and upload.

Uncomment the entire third task, keep it aligned with the other tasks, and complete its three TODO values:

| Property | What to work out |
|---|---|
| `from` | A list containing the Cloud Storage URI produced by `upload_to_gcs`. Use the pattern `{{ outputs.TASK_ID.OUTPUT_NAME }}` with output name `uri`. |
| `destinationTable` | Build `PROJECT.DATASET.yellow_trips_YEAR_MONTH` using the corresponding inputs. |
| `writeDisposition` | Choose the behaviour that replaces the selected month's table when rerunning. |

The remaining settings are provided:

- `projectId`: project where the job runs.
- `location`: dataset location, **EU** in our exercise.
- `format: PARQUET`: the uploaded file format; BigQuery reads the schema from the file.
- `createDisposition: CREATE_IF_NEEDED`: allow the load job to create the table if it does not exist. The dataset must already exist.

Use these choices to decide the rerun behaviour:

| Choice | When the destination table already contains rows |
|---|---|
| `WRITE_APPEND` | Add the file's rows to the existing rows. |
| `WRITE_TRUNCATE` | Replace the table's contents with the loaded data. |
| `WRITE_EMPTY` | Fail instead of writing into a nonempty table. |

For project `my-course-project`, dataset `taxi_analytics`, year `2024`, and month `01`, the destination should be:

```text
my-course-project.taxi_analytics.yellow_trips_2024_01
```

**Why include the month in the table name?** Replacing January's table should not erase February. This is a simple teaching design. A partitioned table holding multiple months is a later design option; do not use one shared table with full-table replacement for all months.

**Explain before executing:** Why does the load task need the upload's `gs://` URI rather than the download's internal Kestra URI? Which service actually reads the Cloud Storage object and creates the table?

## 4. Save and execute the completed flow

Save the edited **`taxi_to_bigquery` flow in Kestra**. Check that the editor recognizes `io.kestra.plugin.gcp.bigquery.LoadFromGcs` and accepts the configuration. Use the installed plugin's documentation if it differs from the latest online documentation; do not upgrade the course environment just to follow a newer example.

Execute with:

| Input | Value |
|---|---|
| `project_id` | Your course project ID, not its display name. |
| `dataset` | `taxi_analytics`, or your actual dataset ID if different. |
| `location` | `EU`, matching the existing dataset. |
| `bucket` | Your actual bucket name, without `gs://`. |
| `year` | `2024` |
| `month` | `01` |

Watch all **three tasks**. The load task should finish successfully. Inspect its outputs for the destination table, job ID and loaded row count where exposed by your installed plugin. A failure in loading does not undo the already completed upload.

Run one execution at a time for a given month; wait for it to finish before starting the rerun. Both executions would otherwise write to the same object and table.

The full flow downloads and uploads again on each execution. That is intentional here: it demonstrates the complete source → storage → table path.

## 5. Inspect and query the table

Open **BigQuery** in the Google Cloud Console. In Explorer, expand your project, then `taxi_analytics`, and select `yellow_trips_2024_01`. Refresh Explorer if necessary.

- **Schema:** inspect the column names and types. These are the original TLC fields, such as `tpep_pickup_datetime` and `fare_amount`; this flow did not rename them to the Week 5 ETL column names.
- **Preview:** look at a few records.
- **Details:** confirm the table exists in the expected dataset.

Open a SQL editor. Replace `YOUR_PROJECT_ID` below with your project ID, and adjust the dataset/table if you used other inputs. Keep the **backticks** around the full table name. Run each query separately.

Count the records:

```sql
SELECT COUNT(*) AS trip_count
FROM `YOUR_PROJECT_ID.taxi_analytics.yellow_trips_2024_01`;
```

Record the count and compare it with the load job's output row count if available. Do not assume that Kestra SUCCESS alone proves the intended table was loaded.

Inspect a few fields:

```sql
SELECT tpep_pickup_datetime,
       tpep_dropoff_datetime,
       trip_distance,
       fare_amount
FROM `YOUR_PROJECT_ID.taxi_analytics.yellow_trips_2024_01`
LIMIT 10;
```

BigQuery executes these queries in Google Cloud, not in local PostgreSQL or the Kestra container. Check the editor's estimated bytes processed before running; `LIMIT 10` limits returned rows, not necessarily the data scanned. For simply browsing records, use Preview.

## 6. Predict and test a rerun

Before rerunning, write down the current row count and predict the next count.

1. Execute the same saved flow again with the same project, dataset, bucket, year and month.
2. Wait until `load_bigquery` succeeds.
3. Run the count query again.
4. Explain whether your chosen `writeDisposition` produced the intended result.

With replacement and an unchanged source file, the count should stay the same. This avoids accumulating rows across identical reruns; it does **not** remove duplicate records already present in the source file. A changed source file can legitimately change the resulting table and count.

**Discuss:** What would happen with `WRITE_APPEND`? What would happen if February used exactly the same destination table name as January with `WRITE_TRUNCATE`?

A successful replacement updates the destination table as one load operation. It does not make the download, Cloud Storage upload and BigQuery load one shared transaction.

## 7. Finish and clean up

If continuing to a later SQL exercise, keep the resources as directed by the instructor. Otherwise:

1. In BigQuery, delete only the monthly exercise table you created, and any other monthly tables you intentionally created during this exercise.
2. In Cloud Storage, delete only the uploaded exercise object(s), following Part 5's notes on versions and soft deletion.
3. Return to your original Terraform directory. With the Terraform key configured, inspect `terraform plan -destroy` and run `terraform destroy` to remove the now-empty dataset and bucket.

The tables were created by the load job, not recorded as separate Terraform resources. Do not enable `delete_contents_on_destroy` or `force_destroy` just to bypass cleanup checks. The manually granted IAM roles and service account remain after destroying the bucket and dataset; remove the pipeline key when it is no longer needed, as described in Part 5.

## If something fails

| Symptom | Check |
|---|---|
| Missing `bigquery.jobs.create` | Grant BigQuery Job User to the pipeline account in the job's project. |
| Table creation or write denied | Check BigQuery Data Editor for the pipeline account and the target dataset/project. |
| Missing `storage.buckets.get` | Check Storage Bucket Viewer; Storage Object User alone does not provide bucket metadata access. |
| Cannot read the source object | Inspect the successful upload output, bucket name, object path and pipeline account's Storage permissions. |
| Dataset not found / location mismatch | Verify the project ID, dataset ID and dataset location. This practical uses EU for both bucket and dataset. |
| Flow succeeds but no table appears | Confirm the third task is uncommented, saved and executed; inspect its destination and the Console's selected project. |
| Row count increases unexpectedly on rerun | Check `writeDisposition`, the destination table name and whether the source file changed. |
| Old column names fail in SQL | Inspect Schema; this load retains TLC names rather than the renamed Week 5 ETL fields. |

**Finish with:** a three-task successful execution, a queryable BigQuery table, and evidence that rerunning the same unchanged month does not accumulate rows.

References: [Kestra LoadFromGcs](https://kestra.io/plugins/plugin-gcp/google-cloud-bigquery/io.kestra.plugin.gcp.bigquery.loadfromgcs) · [Loading Parquet into BigQuery](https://docs.cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet).
