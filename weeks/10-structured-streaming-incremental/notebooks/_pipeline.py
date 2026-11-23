# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline helpers
# MAGIC
# MAGIC The pipeline as it looks at the end of each notebook, in one place. The demos load it with `%run`
# MAGIC (after `_config` and `_generator`) and start with `catch_up(n)`: that rebuilds bronze, silver and gold
# MAGIC to where notebook `n` left them. So every demo starts from the same state, and you can skip one.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

def reset_pipeline():
    for table in [GOLD_TABLE, SILVER_TABLE, BRONZE_TABLE]:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    for folder in [INCOMING_DIR, STATE_VOLUME]:
        try:
            content = dbutils.fs.ls(folder)
        except Exception:
            content = []
        for f in content:
            dbutils.fs.rm(f.path, recurse=True)
    dbutils.fs.mkdirs(INCOMING_DIR)


def run_bronze_with_retry():
    # A new column stops Auto Loader once (notebook 03). A job would retry, we do it here.
    try:
        return run_bronze()
    except RuntimeError as e:
        if "UNKNOWN_FIELD" not in str(e):
            raise
        print("bronze: new column, Auto Loader stopped once and updated its schema. Running again.")
        return run_bronze()


def catch_up(block):
    """Rebuild the pipeline to the state at the end of notebook `block`."""
    print(f"Rebuilding the pipeline as it is after notebook {block:02d} ...")
    reset_pipeline()
    for batch in (1, 2):
        write_batch(batch)
    run_bronze()                          # 01: batches 1 and 2 in bronze
    if block >= 3:
        write_batch(3)
        run_bronze_with_retry()           # 03: batch 3 with the new column tip_chf
    if block >= 4:
        run_silver()                      # 04: silver, one row per order
    if block >= 5:
        for batch in (4, 5):              # 05: Anna cancels, then more orders; gold from the Change Data Feed
            write_batch(batch)
            run_bronze()
            run_silver()
            run_gold()
    summary = [f"{spark.table(BRONZE_TABLE).count()} rows in bronze"]
    if block >= 4:
        summary.append(f"{spark.table(SILVER_TABLE).count()} orders in silver")
    if block >= 5:
        summary.append(f"{spark.table(GOLD_TABLE).count()} restaurants in gold")
    print("Done:", ", ".join(summary))

# COMMAND ----------

def run_bronze():
    # The Auto Loader query of notebook 01 (landing folder → bronze), started through run_stream_once, so that a
    # stop because of a new column comes back as a normal exception (see _stream_once)
    rows = run_stream_once(BRONZE_TABLE, f"{STATE_VOLUME}/bronze_orders", folder=INCOMING_DIR,
                           **{"cloudFiles.schemaEvolutionMode": "addNewColumns"})
    print(f"bronze: {rows} new rows")
    return rows


def run_bronze_inline():
    # The same query as run_bronze, started directly in this notebook instead of through _stream_once. That's what a
    # job wants: a stop because of a new column fails the task, and the job's retry runs it again
    query = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", BRONZE_SCHEMA_LOCATION)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(INCOMING_DIR)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
        .writeStream
        .option("checkpointLocation", BRONZE_CHECKPOINT)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(BRONZE_TABLE)
    )
    query.awaitTermination()
    rows = input_rows(query)
    print(f"bronze: {rows} new rows")
    return rows


def create_silver_table():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
            order_id STRING, customer STRING, restaurant STRING, city STRING,
            items INT, amount_chf DOUBLE, tip_chf DOUBLE, status STRING,
            event_ts TIMESTAMP, updated_at TIMESTAMP
        )
        TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """)


def upsert_to_silver(batch_df, batch_id):
    if "tip_chf" not in batch_df.columns:
        batch_df = batch_df.withColumn("tip_chf", F.lit(None).cast("double"))
    latest = (
        batch_df.where("_rescued_data IS NULL AND order_id IS NOT NULL")
        .select(
            "order_id", "customer", "restaurant", "city",
            F.col("items").cast("int").alias("items"),
            F.col("amount_chf").cast("double").alias("amount_chf"),
            F.col("tip_chf").cast("double").alias("tip_chf"),
            "status",
            F.to_timestamp(F.col("event_ts").cast("string")).alias("event_ts"),
        )
        .withColumn("_rn", F.row_number().over(Window.partitionBy("order_id").orderBy(F.col("event_ts").desc())))
        .where("_rn = 1")
        .drop("_rn")
        .withColumn("updated_at", F.current_timestamp())
    )
    (
        DeltaTable.forName(batch_df.sparkSession, SILVER_TABLE).alias("t")
        .merge(latest.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdateAll(condition="s.event_ts > t.event_ts")
        .whenNotMatchedInsertAll()
        .execute()
    )


def run_silver():
    create_silver_table()
    query = (
        spark.readStream.table(BRONZE_TABLE)
        .writeStream
        .foreachBatch(upsert_to_silver)
        .option("checkpointLocation", SILVER_CHECKPOINT)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    print(f"silver: {input_rows(query)} new bronze rows processed")
    return input_rows(query)

# COMMAND ----------

def create_gold_table():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_TABLE} (
            restaurant STRING, active_orders BIGINT, revenue_chf DOUBLE, updated_at TIMESTAMP
        )
    """)


def apply_changes_to_gold(batch_df, batch_id):
    sign = (
        F.when(F.col("_change_type").isin("insert", "update_postimage"), F.lit(1))
        .when(F.col("_change_type").isin("update_preimage", "delete"), F.lit(-1))
        .otherwise(F.lit(0))
    )
    counts = F.col("status") != "cancelled"
    deltas = (
        batch_df.withColumn("_sign", sign)
        .groupBy("restaurant")
        .agg(
            F.sum(F.when(counts, F.col("_sign")).otherwise(0)).alias("d_orders"),
            F.sum(F.when(counts, F.col("_sign") * F.col("amount_chf")).otherwise(0)).alias("d_revenue"),
        )
    )
    (
        DeltaTable.forName(batch_df.sparkSession, GOLD_TABLE).alias("t")
        .merge(deltas.alias("s"), "t.restaurant = s.restaurant")
        .whenMatchedUpdate(set={
            "active_orders": "t.active_orders + s.d_orders",
            "revenue_chf": "round(t.revenue_chf + s.d_revenue, 2)",
            "updated_at": "current_timestamp()",
        })
        .whenNotMatchedInsert(values={
            "restaurant": "s.restaurant",
            "active_orders": "s.d_orders",
            "revenue_chf": "round(s.d_revenue, 2)",
            "updated_at": "current_timestamp()",
        })
        .execute()
    )


def run_gold():
    create_gold_table()
    query = (
        spark.readStream
        .option("readChangeFeed", "true")
        .table(SILVER_TABLE)
        .writeStream
        .foreachBatch(apply_changes_to_gold)
        .option("checkpointLocation", GOLD_CHECKPOINT)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    print(f"gold: {input_rows(query)} change rows applied")
    return input_rows(query)


def gold_diffs():
    # gold compared with a full recompute from silver, every diff should be 0
    return spark.sql(f"""
        WITH full AS (
            SELECT restaurant,
                   count_if(status != 'cancelled') AS active_orders,
                   round(sum(CASE WHEN status != 'cancelled' THEN amount_chf ELSE 0 END), 2) AS revenue_chf
            FROM {SILVER_TABLE}
            GROUP BY restaurant
        )
        SELECT coalesce(f.restaurant, g.restaurant) AS restaurant,
               coalesce(g.active_orders, 0) - coalesce(f.active_orders, 0) AS diff_orders,
               round(coalesce(g.revenue_chf, 0) - coalesce(f.revenue_chf, 0), 2) AS diff_revenue
        FROM full f FULL OUTER JOIN {GOLD_TABLE} g ON f.restaurant = g.restaurant
        ORDER BY restaurant
    """)


# COMMAND ----------

def run_job_and_wait(w, job_id, timeout_minutes=30):
    # Start a job run and wait until it has finished, successful or not. Returns the run with all its task attempts.
    import time
    waiter = w.jobs.run_now(job_id=job_id)
    run_id = waiter.response.run_id if hasattr(waiter, "response") else waiter.run_id
    deadline = time.time() + timeout_minutes * 60
    while True:
        run = w.jobs.get_run(run_id)
        if run.state.life_cycle_state.value in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            return run
        if time.time() > deadline:
            raise TimeoutError(f"run {run_id} still running after {timeout_minutes} minutes")
        time.sleep(10)


def run_summary(w, run):
    # Result, number of attempts and the notebook output of the last attempt
    result = run.state.result_state.value if run.state.result_state else run.state.life_cycle_state.value
    last = max(run.tasks, key=lambda t: t.attempt_number or 0)     # the attempts don't come sorted
    output = w.jobs.get_run_output(last.run_id).notebook_output
    return result, len(run.tasks), (output.result if output else None)
