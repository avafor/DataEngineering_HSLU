# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 06 – NYC taxi: landing → raw → bronze → silver → gold
# MAGIC
# MAGIC **Slides:** *Medallion Architecture* · *Raw Layer* · *Bronze / Silver / Gold Layer* · *Medallion with a landing zone in
# MAGIC front* · *Ingestion paths: push and pull*
# MAGIC
# MAGIC **Goal (≈ 45 min):** build all layers of a medallion architecture for the whole year 2025.
# MAGIC
# MAGIC ```
# MAGIC volume landing/   volume raw/                       bronze_*                silver_*                gold_*
# MAGIC   (push) ──────►    yellow_tripdata_2025-*.parquet ──► bronze_trips     ──►  silver_trips      ──►  gold_daily_zone
# MAGIC                     taxi_zone_lookup.csv          ──► bronze_zones     ──►  silver_zones
# MAGIC                     payment_types.json            ──► bronze_payment_types (joined into silver_trips)
# MAGIC ```
# MAGIC
# MAGIC - **Landing** = temporary drop zone for suppliers (push). Emptied after pickup.
# MAGIC - **Raw** = the files byte for byte, sorted by source and date. Never changed.
# MAGIC - **Bronze** = the source as a table, plus where and when it was loaded. No cleaning.
# MAGIC - **Silver** = typed, renamed, checked, deduplicated and joined with its lookups.
# MAGIC - **Gold** = aggregated for one question, ready for BI.
# MAGIC
# MAGIC Needs the files from `00_setup`.

# COMMAND ----------

import importlib

from pyspark.sql import functions as F

import config

importlib.reload(config)   # picks up a change to config.py without restarting Python
catalog = config.CATALOG
SCHEMA = "nyc_taxi"


def q(*parts: str) -> str:
    """Backtick-quote each part of a Unity Catalog name."""
    return ".".join(f"`{p}`" for p in parts)


def t(table: str) -> str:
    return q(catalog, SCHEMA, table)


RAW = f"/Volumes/{catalog}/{SCHEMA}/raw"
LANDING = f"/Volumes/{catalog}/{SCHEMA}/landing"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 – Push: landing → raw
# MAGIC
# MAGIC External suppliers get **write-only** access to the landing zone, never to the lake itself. Our pipeline picks the
# MAGIC delivery up, stores it unchanged in raw – sorted by source and delivery date – and deletes it from landing.
# MAGIC
# MAGIC We play the supplier and drop a small file with zone corrections:

# COMMAND ----------

delivery = "zone_corrections.csv"
dbutils.fs.put(f"{LANDING}/{delivery}",
               "LocationID,Borough,Zone,service_zone\n1,EWR,Newark Liberty Airport,EWR\n", overwrite=True)
print("landing:", [f.name for f in dbutils.fs.ls(LANDING)])

# COMMAND ----------

from datetime import date

target_dir = f"{RAW}/deliveries/zone_corrections/{date.today():%Y-%m-%d}"
for f in dbutils.fs.ls(LANDING):
    dbutils.fs.mv(f.path, f"{target_dir}/{f.name}")     # move = copy + delete in landing

print("landing:", [f.name for f in dbutils.fs.ls(LANDING)])
print("raw    :", target_dir, [f.name for f in dbutils.fs.ls(target_dir)])

# COMMAND ----------

# MAGIC %md
# MAGIC The taxi files themselves came the **pull** way: `00_setup` fetched them from GitHub straight into raw.
# MAGIC Databases (JDBC, CDC) and events (Kafka) would be pulled straight into a bronze table instead – they deliver records,
# MAGIC not files.
# MAGIC
# MAGIC ## 2 – Bronze: load the files as they are
# MAGIC
# MAGIC Every bronze table gets two technical columns:
# MAGIC
# MAGIC - `_source_file` from the hidden `_metadata` column – which file a row came from,
# MAGIC - `_ingested_at` – when it was loaded.
# MAGIC
# MAGIC Parquet carries its own schema. The glob `yellow_tripdata_2025-*.parquet` reads all twelve months at once.

# COMMAND ----------

bronze_trips = (
    spark.read.parquet(f"{RAW}/yellow_tripdata_2025-*.parquet")
    .select("*",
            F.col("_metadata.file_name").alias("_source_file"),
            F.current_timestamp().alias("_ingested_at"))
)
bronze_trips.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("bronze_trips"))
spark.table(t("bronze_trips")).groupBy("_source_file").count().orderBy("_source_file").display()

# COMMAND ----------

# MAGIC %md
# MAGIC CSV has no schema. In bronze we read **every column as a string** – no `inferSchema` – so a surprising value can
# MAGIC never make the load fail. Typing happens in silver.

# COMMAND ----------

bronze_zones = (
    spark.read.option("header", "true").csv(f"{RAW}/taxi_zone_lookup.csv")
    .select("*",
            F.col("_metadata.file_name").alias("_source_file"),
            F.current_timestamp().alias("_ingested_at"))
)
bronze_zones.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("bronze_zones"))

bronze_payment_types = (
    spark.read.json(f"{RAW}/payment_types.json")
    .select("*",
            F.col("_metadata.file_name").alias("_source_file"),
            F.current_timestamp().alias("_ingested_at"))
)
bronze_payment_types.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("bronze_payment_types"))

for name in ["bronze_trips", "bronze_zones", "bronze_payment_types"]:
    print(f"{name:22} {spark.table(t(name)).count():>12,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 – Look before you clean
# MAGIC
# MAGIC Which rules does silver need? Check the data, not your assumptions.

# COMMAND ----------

trips = spark.table(t("bronze_trips"))
trips.select(
    F.min("tpep_pickup_datetime").alias("first_pickup"),
    F.max("tpep_pickup_datetime").alias("last_pickup"),
    F.sum(F.when(F.col("trip_distance") <= 0, 1).otherwise(0)).alias("distance_le_0"),
    F.sum(F.when(F.col("total_amount") < 0, 1).otherwise(0)).alias("negative_total"),
    F.sum(F.when(F.col("passenger_count").isNull(), 1).otherwise(0)).alias("passenger_count_null"),
    F.sum(F.when(F.col("tpep_dropoff_datetime") < F.col("tpep_pickup_datetime"), 1).otherwise(0)).alias("dropoff_before_pickup"),
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Typical findings: pickups outside 2025, zero or negative distances, negative amounts (refunds), dropoffs before pickups.
# MAGIC
# MAGIC ## 4 – Silver: typed, cleaned, enriched
# MAGIC
# MAGIC Decisions for this layer:
# MAGIC
# MAGIC - **snake_case names and explicit types** – downstream code should not depend on the source's spelling.
# MAGIC - **Invalid rows are dropped, but counted** – the rules live in one place (`valid`), so the number of rejected rows
# MAGIC   can be reported.
# MAGIC - **Lookups are joined with `left`** and **broadcast** – an unknown zone id must not make a trip disappear, and the
# MAGIC   265 zones are copied to every executor instead of shuffling 49 M trips.
# MAGIC - **Duplicates** are removed on the business key.

# COMMAND ----------

zones = (
    spark.table(t("bronze_zones"))
    .select(F.col("LocationID").cast("int").alias("location_id"),
            F.col("Borough").alias("borough"),
            F.col("Zone").alias("zone"),
            F.col("service_zone"))
)
zones.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("silver_zones"))

payments = spark.table(t("bronze_payment_types")).select(
    F.col("payment_type").cast("int").alias("payment_type"),
    F.col("description").alias("payment_description"),
)

typed = trips.select(
    F.col("VendorID").cast("int").alias("vendor_id"),
    F.col("tpep_pickup_datetime").cast("timestamp").alias("pickup_at"),
    F.col("tpep_dropoff_datetime").cast("timestamp").alias("dropoff_at"),
    F.col("passenger_count").cast("int").alias("passenger_count"),
    F.col("trip_distance").cast("double").alias("trip_distance_mi"),
    F.col("PULocationID").cast("int").alias("pickup_location_id"),
    F.col("DOLocationID").cast("int").alias("dropoff_location_id"),
    F.col("payment_type").cast("int").alias("payment_type"),
    F.col("fare_amount").cast("decimal(10,2)").alias("fare_amount"),
    F.col("tip_amount").cast("decimal(10,2)").alias("tip_amount"),
    F.col("cbd_congestion_fee").cast("decimal(10,2)").alias("cbd_fee"),
    F.col("total_amount").cast("decimal(10,2)").alias("total_amount"),
    "_source_file",
)

valid = (
    (F.col("pickup_at") >= F.lit("2025-01-01").cast("timestamp"))
    & (F.col("pickup_at") < F.lit("2026-01-01").cast("timestamp"))
    & (F.col("dropoff_at") >= F.col("pickup_at"))
    & (F.col("trip_distance_mi") > 0)
    & (F.col("total_amount") >= 0)
)

clean = typed.filter(valid).dropDuplicates(
    ["vendor_id", "pickup_at", "dropoff_at", "pickup_location_id", "dropoff_location_id", "total_amount"]
)

pu = zones.select(F.col("location_id").alias("pickup_location_id"),
                  F.col("borough").alias("pickup_borough"), F.col("zone").alias("pickup_zone"))
dz = zones.select(F.col("location_id").alias("dropoff_location_id"),
                  F.col("borough").alias("dropoff_borough"), F.col("zone").alias("dropoff_zone"))

silver = (
    clean
    .join(F.broadcast(pu), "pickup_location_id", "left")
    .join(F.broadcast(dz), "dropoff_location_id", "left")
    .join(F.broadcast(payments), "payment_type", "left")
    .withColumn("pickup_date", F.to_date("pickup_at"))
    .withColumn("duration_min", F.round((F.unix_timestamp("dropoff_at") - F.unix_timestamp("pickup_at")) / 60, 1))
)
silver.createOrReplaceTempView("silver_trips_staging")

# COMMAND ----------

# MAGIC %md
# MAGIC The table is written with SQL so that it can be **liquid-clustered** on `pickup_date`: queries that filter on a day
# MAGIC or a month then skip most files – the min/max effect from notebook 03. Unlike `PARTITIONED BY`, clustering keys can
# MAGIC be changed later and do not create one folder per value.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {t('silver_trips')}
    CLUSTER BY (pickup_date)
    AS SELECT * FROM silver_trips_staging
""")

bronze_count = trips.count()
silver_count = spark.table(t("silver_trips")).count()
print(f"bronze_trips : {bronze_count:>12,}")
print(f"silver_trips : {silver_count:>12,}")
print(f"rejected     : {bronze_count - silver_count:>12,}  ({(bronze_count - silver_count) / bronze_count:.1%})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 – Gold: aggregated for one question
# MAGIC
# MAGIC *How many trips and how much revenue per day and pickup zone?* – the table behind a dashboard. Small, stable column
# MAGIC names, one row per day and zone.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {t('gold_daily_zone')} AS
    SELECT pickup_date,
           pickup_borough,
           pickup_zone,
           COUNT(*)                         AS trips,
           ROUND(SUM(total_amount), 2)      AS revenue,
           ROUND(AVG(trip_distance_mi), 2)  AS avg_distance_mi
    FROM {t('silver_trips')}
    GROUP BY pickup_date, pickup_borough, pickup_zone
""")
print(f"gold_daily_zone: {spark.table(t('gold_daily_zone')).count():,} rows")
spark.table(t("gold_daily_zone")).orderBy(F.desc("revenue")).limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 – Analysis on silver
# MAGIC
# MAGIC Two business questions, once in SQL and once in PySpark – same engine, same plan.

# COMMAND ----------

spark.sql(f"USE CATALOG {q(catalog)}")
spark.sql(f"USE SCHEMA {q(SCHEMA)}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Trips and revenue per pickup borough
# MAGIC SELECT pickup_borough,
# MAGIC        COUNT(*)                        AS trips,
# MAGIC        ROUND(SUM(total_amount), 0)     AS revenue,
# MAGIC        ROUND(AVG(trip_distance_mi), 2) AS avg_distance_mi
# MAGIC FROM silver_trips
# MAGIC GROUP BY pickup_borough
# MAGIC ORDER BY trips DESC

# COMMAND ----------

# Average duration and trips per hour of day
(spark.table(t("silver_trips"))
 .groupBy(F.hour("pickup_at").alias("hour"))
 .agg(F.count("*").alias("trips"), F.round(F.avg("duration_min"), 1).alias("avg_duration_min"))
 .orderBy("hour")
 .display())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 – Exercises

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 6.1 – Which rule rejects how many trips?
# MAGIC
# MAGIC `valid` combines five rules. Count, on `typed`, how many trips break **each** rule on its own (one column per rule).
# MAGIC Which rule rejects the most?

# COMMAND ----------

# TODO Task 6.1 – rejected trips per silver rule
raise NotImplementedError("Task 6.1")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>One <code>F.sum(F.when(condition, 1).otherwise(0))</code> per rule, all in one <code>select</code> – one pass over the data.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>rules = {&quot;distance_le_0&quot;: F.col(&quot;trip_distance_mi&quot;) &lt;= 0, ___}
# MAGIC typed.select([F.sum(F.when(c, 1).otherwise(0)).alias(n) for n, c in rules.items()]).display()</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>rules = {
# MAGIC     &quot;pickup_outside_2025&quot;: (F.col(&quot;pickup_at&quot;) &lt; F.lit(&quot;2025-01-01&quot;).cast(&quot;timestamp&quot;))
# MAGIC                            | (F.col(&quot;pickup_at&quot;) &gt;= F.lit(&quot;2026-01-01&quot;).cast(&quot;timestamp&quot;)),
# MAGIC     &quot;dropoff_before_pickup&quot;: F.col(&quot;dropoff_at&quot;) &lt; F.col(&quot;pickup_at&quot;),
# MAGIC     &quot;distance_le_0&quot;: F.col(&quot;trip_distance_mi&quot;) &lt;= 0,
# MAGIC     &quot;negative_total&quot;: F.col(&quot;total_amount&quot;) &lt; 0,
# MAGIC }
# MAGIC typed.select([F.sum(F.when(cond, 1).otherwise(0)).alias(name) for name, cond in rules.items()]).display()</code></pre></details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 6.2 – A second gold table
# MAGIC
# MAGIC Create `gold_hourly_borough`: trips, revenue and average tip per **pickup borough and hour of day** (0–23), from
# MAGIC `silver_trips`. Show the busiest hour of each borough.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO Task 6.2 – gold_hourly_borough
# MAGIC SELECT 'replace this line with your query' AS todo

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>CREATE OR REPLACE TABLE … AS SELECT … GROUP BY pickup_borough, HOUR(pickup_at)</code>. For the busiest hour per borough, <code>MAX_BY(hour, trips)</code> returns the hour with the most trips.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>CREATE OR REPLACE TABLE gold_hourly_borough AS
# MAGIC SELECT pickup_borough,
# MAGIC        HOUR(pickup_at)             AS hour,
# MAGIC        COUNT(*)                    AS trips,
# MAGIC        ROUND(SUM(total_amount), 2) AS revenue,
# MAGIC        ROUND(AVG(tip_amount), 2)   AS avg_tip
# MAGIC FROM silver_trips
# MAGIC GROUP BY pickup_borough, HOUR(pickup_at);
# MAGIC 
# MAGIC SELECT pickup_borough, MAX_BY(hour, trips) AS busiest_hour, MAX(trips) AS trips
# MAGIC FROM gold_hourly_borough
# MAGIC GROUP BY pickup_borough
# MAGIC ORDER BY trips DESC;</code></pre></details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 6.3 – The congestion fee
# MAGIC
# MAGIC Since January 2025 trips into Manhattan's central business district pay `cbd_fee`. From `silver_trips`, compute per
# MAGIC month the number of trips with a fee > 0 and the fee revenue. In PySpark this time.

# COMMAND ----------

# TODO Task 6.3 – congestion fee per month
raise NotImplementedError("Task 6.3")
# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Filter <code>cbd_fee &gt; 0</code>, group by <code>F.date_trunc(&quot;month&quot;, &quot;pickup_at&quot;)</code>, count and sum.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>(spark.table(t(&quot;silver_trips&quot;))
# MAGIC  .filter(F.col(&quot;cbd_fee&quot;) &gt; 0)
# MAGIC  .groupBy(F.date_trunc(&quot;month&quot;, &quot;pickup_at&quot;).alias(&quot;month&quot;))
# MAGIC  .agg(F.count(&quot;*&quot;).alias(&quot;trips_with_fee&quot;), F.round(F.sum(&quot;cbd_fee&quot;), 0).alias(&quot;fee_revenue&quot;))
# MAGIC  .orderBy(&quot;month&quot;)
# MAGIC  .display())</code></pre></details>

