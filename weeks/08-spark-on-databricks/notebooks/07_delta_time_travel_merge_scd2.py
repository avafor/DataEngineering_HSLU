# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 07 – Delta Lake: time travel, MERGE and SCD2
# MAGIC
# MAGIC **Slides:** *What table formats add* · *Delta Lake: one log, many versions* – here on a real dimension table.
# MAGIC
# MAGIC **Goal (≈ 30 min):** see what a table format adds on top of Parquet files:
# MAGIC
# MAGIC | Section | Feature |
# MAGIC |---|---|
# MAGIC | 1 | Every write is a new **version** in the transaction log |
# MAGIC | 2 | **Time travel** and **RESTORE** after a faulty `UPDATE` |
# MAGIC | 3 | **MERGE** – upsert changes in one atomic statement |
# MAGIC | 4 | **SCD2** – keep the history of a dimension with MERGE |
# MAGIC
# MAGIC Uses `silver_zones` and `silver_trips` from notebook 06. Run 06 first, with the same `catalog`.

# COMMAND ----------

import importlib

import config

importlib.reload(config)   # picks up a change to config.py without restarting Python
catalog = config.CATALOG

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql("USE SCHEMA nyc_taxi")
print("Working in", spark.sql("SELECT current_catalog() || '.' || current_schema()").first()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 – A playground table
# MAGIC
# MAGIC The table is dropped first, so its first version is always **0** – also when you re-run the notebook.
# MAGIC (`CREATE OR REPLACE` would keep the old history and shift the version numbers.)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS zones_playground;
# MAGIC CREATE TABLE zones_playground AS SELECT * FROM silver_zones;
# MAGIC SELECT * FROM zones_playground ORDER BY location_id LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 – A mistake, the history, and the way back
# MAGIC
# MAGIC An `UPDATE` without `WHERE` – the classic. With plain Parquet files the old values would be gone.

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE zones_playground SET borough = 'Manhattan';   -- forgot the WHERE clause
# MAGIC SELECT borough, COUNT(*) AS zones FROM zones_playground GROUP BY borough;

# COMMAND ----------

# MAGIC %md
# MAGIC Every write is a commit in the **transaction log** (`_delta_log/`). `DESCRIBE HISTORY` lists them:

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY zones_playground

# COMMAND ----------

# MAGIC %md
# MAGIC **Time travel:** read the table as it was at an earlier version (or `TIMESTAMP AS OF '…'`).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT borough, COUNT(*) AS zones
# MAGIC FROM zones_playground VERSION AS OF 0
# MAGIC GROUP BY borough
# MAGIC ORDER BY zones DESC

# COMMAND ----------

# MAGIC %md
# MAGIC The same in PySpark:

# COMMAND ----------

spark.read.option("versionAsOf", 0).table("zones_playground").groupBy("borough").count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC **RESTORE** brings the table back. It does not delete the faulty version – it adds a new version
# MAGIC whose content equals version 0. The history stays complete.

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE zones_playground TO VERSION AS OF 0;
# MAGIC DESCRIBE HISTORY zones_playground;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 – MERGE: upsert in one statement
# MAGIC
# MAGIC A delivery of changes arrives: one zone is renamed, one is new. `MERGE` updates matching rows and
# MAGIC inserts the others – atomically, so a reader never sees half of the change.
# MAGIC
# MAGIC **These changes are made up for the exercise.** The real TLC zone list did not change in 2025:
# MAGIC zone 1 is still "Newark Airport", and there is no zone 266.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW zone_changes AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (1,   'EWR',       'Newark Liberty Airport', 'EWR'),
# MAGIC   (266, 'Manhattan', 'Hudson Yards Heliport',  'Yellow Zone')
# MAGIC AS c(location_id, borough, zone, service_zone);
# MAGIC
# MAGIC MERGE INTO zones_playground AS t
# MAGIC USING zone_changes AS s
# MAGIC   ON t.location_id = s.location_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM zones_playground WHERE location_id IN (1, 266);

# COMMAND ----------

# MAGIC %md
# MAGIC The old name of zone 1 is gone from the current table (only time travel still knows it).
# MAGIC That is **SCD1** – overwrite. When reports must show the name that was valid *at the time of a
# MAGIC trip*, the dimension needs **SCD2**.
# MAGIC
# MAGIC ## 4 – SCD2: keep every version of a row
# MAGIC
# MAGIC Each row gets a validity range and a current flag:
# MAGIC
# MAGIC | location_id | zone | valid_from | valid_to | is_current |
# MAGIC |---|---|---|---|---|
# MAGIC | 1 | Newark Airport | 2025-01-01 | 2025-07-01 | false |
# MAGIC | 1 | Newark Liberty Airport | 2025-07-01 | NULL | true |
# MAGIC
# MAGIC Initial load: every zone is valid from the start of the data.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dim_zone_scd2;
# MAGIC CREATE TABLE dim_zone_scd2 AS
# MAGIC SELECT location_id, borough, zone, service_zone,
# MAGIC        DATE'2025-01-01'  AS valid_from,
# MAGIC        CAST(NULL AS DATE) AS valid_to,
# MAGIC        true              AS is_current
# MAGIC FROM silver_zones;

# COMMAND ----------

# MAGIC %md
# MAGIC **The trick:** a changed row needs *two* actions – close the old version (UPDATE) and insert the
# MAGIC new one (INSERT). One `MERGE` can do only one action per source row, so every changed row is fed
# MAGIC in **twice**:
# MAGIC
# MAGIC - with `merge_key = location_id` → matches the current row → closes it,
# MAGIC - with `merge_key = NULL` → never matches → inserted as the new current version.
# MAGIC
# MAGIC New zones come in once (no match → insert). Unchanged rows match, but the `AND` condition skips them.
# MAGIC `<=>` is the null-safe equality, so a NULL value is compared correctly too.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW zone_changes_dated AS
# MAGIC SELECT *, DATE'2025-07-01' AS change_date FROM zone_changes;
# MAGIC
# MAGIC MERGE INTO dim_zone_scd2 AS t
# MAGIC USING (
# MAGIC   SELECT c.location_id AS merge_key, c.* FROM zone_changes_dated c
# MAGIC   UNION ALL
# MAGIC   SELECT NULL AS merge_key, c.*
# MAGIC   FROM zone_changes_dated c
# MAGIC   JOIN dim_zone_scd2 d
# MAGIC     ON c.location_id = d.location_id AND d.is_current
# MAGIC   WHERE NOT (c.borough <=> d.borough AND c.zone <=> d.zone AND c.service_zone <=> d.service_zone)
# MAGIC ) AS s
# MAGIC   ON t.location_id = s.merge_key AND t.is_current
# MAGIC WHEN MATCHED AND NOT (t.borough <=> s.borough AND t.zone <=> s.zone AND t.service_zone <=> s.service_zone) THEN
# MAGIC   UPDATE SET t.is_current = false, t.valid_to = s.change_date
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (location_id, borough, zone, service_zone, valid_from, valid_to, is_current)
# MAGIC   VALUES (s.location_id, s.borough, s.zone, s.service_zone, s.change_date, NULL, true);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dim_zone_scd2 WHERE location_id IN (1, 266) ORDER BY location_id, valid_from;

# COMMAND ----------

# MAGIC %md
# MAGIC **Run the MERGE cell a second time:** nothing changes. The statement is **idempotent** – a
# MAGIC repeated delivery does not create duplicate versions. Check it with the cell below.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT location_id, COUNT(*) AS versions, SUM(CAST(is_current AS INT)) AS current_rows
# MAGIC FROM dim_zone_scd2
# MAGIC GROUP BY location_id
# MAGIC HAVING COUNT(*) > 1 OR SUM(CAST(is_current AS INT)) <> 1;

# COMMAND ----------

# MAGIC %md
# MAGIC **Point-in-time lookup:** which zone name was valid on the day of a trip?
# MAGIC
# MAGIC ```sql
# MAGIC SELECT t.pickup_at, d.zone
# MAGIC FROM silver_trips t
# MAGIC JOIN dim_zone_scd2 d
# MAGIC   ON  t.pickup_location_id = d.location_id
# MAGIC   AND t.pickup_date >= d.valid_from
# MAGIC   AND (t.pickup_date < d.valid_to OR d.valid_to IS NULL)
# MAGIC ```
# MAGIC
# MAGIC ## 5 – Exercises

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 7.1 – Look at the mistake
# MAGIC
# MAGIC Query `zones_playground` **as of version 1** (right after the `UPDATE` without `WHERE`) and count the zones per
# MAGIC borough. Then do the same for the current version.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO Task 7.1 – boroughs at version 1 and now
# MAGIC SELECT 'replace this line with your query' AS todo

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>SELECT … FROM zones_playground VERSION AS OF 1 GROUP BY borough</code> – and the same without <code>VERSION AS OF</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT &#x27;version 1&#x27; AS state, borough, COUNT(*) AS zones FROM zones_playground VERSION AS OF 1 GROUP BY borough
# MAGIC UNION ALL
# MAGIC SELECT &#x27;current&#x27;, borough, COUNT(*) FROM zones_playground GROUP BY borough
# MAGIC ORDER BY state, zones DESC;</code></pre></details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 7.2 – Your own MERGE
# MAGIC
# MAGIC A second (made-up) delivery: zone **2** becomes `'Jamaica Bay Wildlife Refuge'`, and a new zone **267**
# MAGIC `'Queens', 'Willets Point', 'Boro Zone'` appears. Upsert it into `zones_playground` with `MERGE` and read the
# MAGIC `operationMetrics` of the new version in `DESCRIBE HISTORY`: how many rows were updated and inserted?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO Task 7.2 – second MERGE and its metrics
# MAGIC SELECT 'replace this line with your query' AS todo

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Same MERGE as in section 3 with a different <code>VALUES</code> list. <code>DESCRIBE HISTORY</code> can be used as a subquery: <code>SELECT … FROM (DESCRIBE HISTORY zones_playground)</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>MERGE INTO zones_playground AS t
# MAGIC USING (SELECT * FROM VALUES
# MAGIC          (2,   &#x27;Queens&#x27;, &#x27;Jamaica Bay Wildlife Refuge&#x27;, &#x27;Boro Zone&#x27;),
# MAGIC          (267, &#x27;Queens&#x27;, &#x27;Willets Point&#x27;,               &#x27;Boro Zone&#x27;)
# MAGIC        AS c(location_id, borough, zone, service_zone)) AS s
# MAGIC   ON t.location_id = s.location_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC 
# MAGIC SELECT version, operation,
# MAGIC        operationMetrics.numTargetRowsUpdated  AS updated,
# MAGIC        operationMetrics.numTargetRowsInserted AS inserted
# MAGIC FROM (DESCRIBE HISTORY zones_playground)
# MAGIC ORDER BY version DESC
# MAGIC LIMIT 1;</code></pre></details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 7.3 – Point in time
# MAGIC
# MAGIC Use the point-in-time join above: count the trips that started in zone **1** per month of 2025, together with the
# MAGIC zone name that was valid at that time. Where does the name change?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO Task 7.3 – trips from zone 1 per month with the valid zone name
# MAGIC SELECT 'replace this line with your query' AS todo

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Take the point-in-time join from the markdown cell, filter <code>t.pickup_location_id = 1</code> and group by month and <code>d.zone</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT DATE_TRUNC(&#x27;month&#x27;, t.pickup_date) AS month, d.zone, COUNT(*) AS trips
# MAGIC FROM silver_trips t
# MAGIC JOIN dim_zone_scd2 d
# MAGIC   ON  t.pickup_location_id = d.location_id
# MAGIC   AND t.pickup_date &gt;= d.valid_from
# MAGIC   AND (t.pickup_date &lt; d.valid_to OR d.valid_to IS NULL)
# MAGIC WHERE t.pickup_location_id = 1
# MAGIC GROUP BY DATE_TRUNC(&#x27;month&#x27;, t.pickup_date), d.zone
# MAGIC ORDER BY month;</code></pre></details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean up (optional)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS zones_playground;
# MAGIC -- DROP TABLE IF EXISTS dim_zone_scd2;
