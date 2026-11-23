# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 09 – Databricks and Unity Catalog
# MAGIC
# MAGIC **Slides:** *Databricks: control plane, compute, storage* · *Unity Catalog: one namespace for data and AI* ·
# MAGIC *Unity Catalog: governance*
# MAGIC
# MAGIC **Goal (≈ 20 min):**
# MAGIC
# MAGIC | Section | Idea |
# MAGIC |---|---|
# MAGIC | 1 | Where things live: control plane, compute, your storage |
# MAGIC | 2 | One namespace: catalog → schema → tables, views, volumes |
# MAGIC | 3 | Find and understand data: comments and `DESCRIBE` |
# MAGIC | 4 | Least privilege: `GRANT` and `SHOW GRANTS` |
# MAGIC | 5 | Where did the data come from? Lineage in the system tables |
# MAGIC | 6 | Exercises |
# MAGIC
# MAGIC Needs the tables from `06_nyc_taxi_medallion`.

# COMMAND ----------

import importlib

import config

importlib.reload(config)   # picks up a change to config.py without restarting Python
catalog = config.CATALOG

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql("USE SCHEMA nyc_taxi")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 – Control plane, compute, storage
# MAGIC
# MAGIC - The **control plane** (Databricks' account) runs the web UI, the jobs scheduler, the cluster manager and
# MAGIC   **Unity Catalog** – names, owners, permissions.
# MAGIC - **Compute** runs your Spark code: serverless (in Databricks' account) or classic clusters (VMs in your cloud account).
# MAGIC - **Storage** holds the data: Delta tables, volumes and raw files in object storage.
# MAGIC
# MAGIC Stop all compute and nothing is lost. A managed table knows where its files are:

# COMMAND ----------

(spark.sql("DESCRIBE TABLE EXTENDED silver_trips")
 .where("col_name IN ('Catalog', 'Database', 'Table', 'Type', 'Location', 'Provider', 'Owner')")
 .display())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 – One namespace: catalog → schema → objects
# MAGIC
# MAGIC Every table has a **three-part name** `catalog.schema.table`. Files live in **volumes** under
# MAGIC `/Volumes/catalog/schema/volume/` – governed like tables.

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN `{catalog}`"))
display(spark.sql("SHOW TABLES IN nyc_taxi"))
display(spark.sql("SHOW VOLUMES IN nyc_taxi"))

# COMMAND ----------

spark.sql(f"SELECT COUNT(*) AS trips FROM `{catalog}`.nyc_taxi.silver_trips").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 – Find and understand data
# MAGIC
# MAGIC Comments are stored in Unity Catalog and shown in **Catalog Explorer** and in the search. Write them where they are
# MAGIC cheapest to write: right after the table is created.

# COMMAND ----------

spark.sql("COMMENT ON TABLE silver_trips IS 'NYC yellow taxi trips 2025: typed, checked, deduplicated, joined with zones'")
spark.sql("ALTER TABLE silver_trips ALTER COLUMN cbd_fee COMMENT 'Congestion relief zone toll, charged since 2025-01-05'")
spark.sql("DESCRIBE TABLE silver_trips").where("col_name IN ('cbd_fee', 'pickup_zone')").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 – Least privilege: `GRANT`
# MAGIC
# MAGIC To read a table, a user needs **three** privileges: `USE CATALOG`, `USE SCHEMA` and `SELECT`. Grant them to groups,
# MAGIC not to people. The group `account users` contains every user of the account:

# COMMAND ----------

statements = [
    f"GRANT USE CATALOG ON CATALOG `{catalog}` TO `account users`",
    f"GRANT USE SCHEMA ON SCHEMA `{catalog}`.nyc_taxi TO `account users`",
    f"GRANT SELECT ON TABLE `{catalog}`.nyc_taxi.gold_daily_zone TO `account users`",
]
for statement in statements:
    try:
        spark.sql(statement)
        print("ok  ", statement)
    except Exception as e:
        print("fail", statement, "–", str(e).splitlines()[0][:200])

spark.sql(f"SHOW GRANTS ON TABLE `{catalog}`.nyc_taxi.gold_daily_zone").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 – Lineage: where did the data come from?
# MAGIC
# MAGIC Unity Catalog records which table was written from which, down to the column, in the system table
# MAGIC `system.access.table_lineage`. The entries appear a few minutes after a write.

# COMMAND ----------

try:
    spark.sql(f"""
        SELECT source_table_full_name, target_table_full_name, MAX(event_time) AS last_seen
        FROM system.access.table_lineage
        WHERE target_table_catalog = '{catalog}'
        GROUP BY source_table_full_name, target_table_full_name
        ORDER BY target_table_full_name, source_table_full_name
    """).display()
except Exception as e:
    print("Lineage system table not readable here:", str(e).splitlines()[0][:200])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 – Exercises

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 9.1 – Document the gold table
# MAGIC
# MAGIC Add a comment to `gold_daily_zone` and to its columns `trips` and `revenue`, then show them with `DESCRIBE TABLE`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO Task 9.1 – comments on gold_daily_zone
# MAGIC SELECT 'replace this line with your query' AS todo

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>COMMENT ON TABLE t IS &#x27;…&#x27;</code> and <code>ALTER TABLE t ALTER COLUMN c COMMENT &#x27;…&#x27;</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>COMMENT ON TABLE gold_daily_zone IS &#x27;Trips and revenue per day and pickup zone, 2025&#x27;;
# MAGIC ALTER TABLE gold_daily_zone ALTER COLUMN trips COMMENT &#x27;Number of valid trips&#x27;;
# MAGIC ALTER TABLE gold_daily_zone ALTER COLUMN revenue COMMENT &#x27;Sum of total_amount in USD&#x27;;
# MAGIC DESCRIBE TABLE gold_daily_zone;</code></pre></details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 9.2 – Read access for analysts
# MAGIC
# MAGIC Write the `GRANT` statements that let a group `analysts` read **only** `gold_daily_zone` – nothing else in the catalog.
# MAGIC Run them; on Free Edition the group may not exist, so catch the error and print the statements.

# COMMAND ----------

# TODO Task 9.2 – least privilege for analysts
raise NotImplementedError("Task 9.2")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Three privileges are needed to read one table: <code>USE CATALOG</code>, <code>USE SCHEMA</code>, <code>SELECT</code> – each on its own level.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>analyst_grants = [
# MAGIC     f&quot;GRANT USE CATALOG ON CATALOG `{catalog}` TO `analysts`&quot;,
# MAGIC     f&quot;GRANT USE SCHEMA ON SCHEMA `{catalog}`.nyc_taxi TO `analysts`&quot;,
# MAGIC     f&quot;GRANT SELECT ON TABLE `{catalog}`.nyc_taxi.gold_daily_zone TO `analysts`&quot;,
# MAGIC ]
# MAGIC for statement in analyst_grants:
# MAGIC     try:
# MAGIC         spark.sql(statement)
# MAGIC         print(&quot;ok  &quot;, statement)
# MAGIC     except Exception as e:
# MAGIC         print(&quot;fail&quot;, statement, &quot;–&quot;, str(e).splitlines()[0][:120])</code></pre></details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 9.3 – Which tables feed `gold_daily_zone`?
# MAGIC
# MAGIC Query `system.access.table_lineage` for all source tables of `gold_daily_zone`, and the sources of those sources
# MAGIC (two levels). If the system table is not readable, explain the chain from the notebook 06 code instead.

# COMMAND ----------

# TODO Task 9.3 – two levels of lineage
raise NotImplementedError("Task 9.3")
# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Read <code>system.access.table_lineage</code> once, take the rows whose target is <code>gold_daily_zone</code> (level 1), then the rows whose target is one of those sources (level 2).</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>gold = f&quot;{catalog}.nyc_taxi.gold_daily_zone&quot;
# MAGIC try:
# MAGIC     lineage = spark.sql(f&quot;&quot;&quot;
# MAGIC         SELECT DISTINCT source_table_full_name AS source, target_table_full_name AS target
# MAGIC         FROM system.access.table_lineage
# MAGIC         WHERE target_table_catalog = &#x27;{catalog}&#x27; AND source_table_full_name IS NOT NULL
# MAGIC     &quot;&quot;&quot;)
# MAGIC     level1 = lineage.where(f&quot;target = &#x27;{gold}&#x27;&quot;)
# MAGIC     level2 = lineage.join(level1.selectExpr(&quot;source AS target&quot;), &quot;target&quot;)
# MAGIC     level1.unionByName(level2).display()
# MAGIC except Exception as e:
# MAGIC     print(&quot;Lineage not readable:&quot;, str(e).splitlines()[0][:120])
# MAGIC     print(&quot;From the code: gold_daily_zone ← silver_trips ← bronze_trips, bronze_zones (via silver_zones), bronze_payment_types&quot;)</code></pre></details>

