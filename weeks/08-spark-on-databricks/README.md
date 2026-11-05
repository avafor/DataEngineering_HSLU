# Week 8 — Batch processing with Spark on Databricks

[Module homepage](../../README.md) · [Preparation](../../preparation/week-08.md)

## Goal

Process the full NYC Taxi year 2025 (≈ 49 million trips, 12 Parquet files) with Apache Spark on Databricks Free Edition, and see on real data what the slides explain. By the end of the practical, you should be able to:

- explain why one machine runs out of memory and time, and how Spark splits the work into partitions;
- distinguish transformations from actions and read a query plan;
- explain why Parquet reads faster than CSV (columns, row groups, min/max statistics);
- explain what a table format such as Delta Lake adds to a folder of Parquet files;
- use schema enforcement and schema evolution deliberately;
- build a medallion pipeline from landing to gold; and
- recognise the common Spark pitfalls in a query plan.

## Before you start

You need the Databricks Free Edition account from the [Week 8 preparation](../../preparation/week-08.md). Nothing is installed on your laptop; the data is downloaded directly into your workspace.

## Get the notebooks into your workspace

1. In Databricks, open **Workspace** → **Users** → your email.
2. Click **Create** → **Git folder**.
3. Paste `https://github.com/avafor/DataEngineering_HSLU`, choose **GitHub** as provider and click **Create Git folder**. The repository is public, so no GitHub account or credentials are needed.
4. Open `weeks/08-spark-on-databricks/notebooks`.

When the materials are updated, open the Git folder and click the branch name → **Pull** to get the latest version. Your own edits in the notebooks may conflict with an update; copy a notebook first if you want to keep your solutions.

## Run the notebooks

Attach each notebook to **Serverless** (top right) and run it from top to bottom. Work through them in order:

| Notebook | What you see | Time |
|---|---|---|
| `00_setup` | Creates the catalog `denghs26` with the schemas `nyc_taxi` and `playground` and three volumes, then downloads the taxi data 2025 and the zone lookup (≈ 830 MB) | ≈ 2 min |
| `01_why_spark` | How much memory pandas needs for one month, and for the year; the same question with Spark; partitions and part files | 20 min |
| `02_spark_basics` | Processing model, lazy evaluation, query plans, pandas vs. Spark DataFrames | 25 min |
| `03_parquet_vs_csv` | The same month as CSV and as Parquet: size, footer statistics, column pruning, skipping row groups | 25 min |
| `04_data_lake_to_delta` | A plain Parquet folder vs. a Delta table: the transaction log, versions, time travel, `VACUUM` | 30 min |
| `05_schema_enforcement_evolution` | When Delta rejects a delivery, when it silently drops a column, and how to evolve the schema on purpose | 20 min |
| `06_nyc_taxi_medallion` | The full year from landing → raw → bronze → silver → gold | 45 min |
| `07_delta_time_travel_merge_scd2` | Time travel, `RESTORE`, `MERGE` and a slowly changing dimension (SCD2) | 30 min |
| `08_spark_pitfalls` | The six pitfalls from the appendix, each in a query plan | 30 min |
| `09_unity_catalog` | Three-part names, volumes, comments, permissions and lineage | 20 min |
| `99_cleanup` | Deletes the whole catalog — **only at the end of the module** | – |

`07` and `09` need the tables built in `06`. `08` uses `samples.nyctaxi.trips` and runs on its own.

## Exercises

Each notebook ends with exercises. An exercise cell stops with `NotImplementedError: Task X.Y` until you replace that line with your solution. Below every exercise you find **hints** and the **solution**, folded away: try the task first, then open the hints, and the solution only to compare.

## The catalog name

All notebooks read the catalog from [`notebooks/config.py`](notebooks/config.py):

```python
CATALOG = "denghs26"
```

Keep the default unless you are asked to use a different catalog. If you change it, change it there once, then run `00_setup` again: it creates the new catalog and downloads the data into it.

## If something goes wrong

- **`ModuleNotFoundError: No module named 'config'`** — the notebook is not in the same folder as `config.py`. Open the notebooks inside the Git folder; do not copy single notebooks elsewhere.
- **`00_setup` fails during the download** — run it again; files that are already complete are skipped.
- **A notebook says a table does not exist** — run the notebooks in order; `07` and `09` need `06`.
- **The first cell takes a minute** — serverless compute is starting; this is normal on Free Edition.
