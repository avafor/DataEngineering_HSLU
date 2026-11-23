# Week 8 — Preparation before class

[All preparation checklists](README.md) · [Module homepage](../README.md)

In Week 8, you will process the NYC Taxi data with Apache Spark on **Databricks Free Edition**. You only need a web browser; nothing is installed on your laptop and no data needs to be downloaded.

## 1. Create your account

1. Open the [Databricks Free Edition sign-up](https://login.databricks.com/?intent=CE_SIGN_UP).
2. Sign up with email, Google or Microsoft. Databricks creates your personal workspace.
3. Bookmark the workspace address (`https://dbc-….cloud.databricks.com/`).

Use **Free Edition**, not the 14-day *Free Trial*.

## 2. Check that it works

In **SQL Editor**, run:

```sql
SELECT current_user(), COUNT(*) FROM samples.nyctaxi.trips;
```

You should see your email and a number. The first query may take up to a minute.

`samples.nyctaxi.trips` is only a small demo dataset that every workspace includes; it is used here as a connection test. In class, we work with the full NYC Taxi data of 2025.

Do not create catalogs, schemas or tables yet.
