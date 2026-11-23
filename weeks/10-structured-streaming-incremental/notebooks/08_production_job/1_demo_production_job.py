# Databricks notebook source
# MAGIC %md
# MAGIC # 08 Who runs it every 10 minutes? (demo)
# MAGIC
# MAGIC Our pipeline works: files → bronze → silver → gold, every layer incremental. So far it ran because you pressed
# MAGIC *Run*. The question for this last notebook: **how does it run on its own, and what happens when something goes
# MAGIC wrong at night?**
# MAGIC
# MAGIC ![A job starts the pipeline every 10 minutes: bronze, silver and gold run once with availableNow, each reads only what is new since its checkpoint, then the check against a full recompute. A retry handles the stop on a new column](./assets/pipeline_job.png)

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

# MAGIC %run ../_generator

# COMMAND ----------

# MAGIC %run ../_pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC The pipeline as notebook 05 left it: batches 1 to 5 through bronze, silver and gold. Takes a few minutes.

# COMMAND ----------

catch_up(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Without a job
# MAGIC
# MAGIC - **Someone presses Run.** Works on weekdays. On Sunday gold is a day old, and nobody notices.
# MAGIC - **A loop in a notebook**, `while True: run_pipeline(); time.sleep(600)`. The notebook has to stay open, the
# MAGIC   compute has to run all day, and when the loop dies, nobody gets told. Free Edition stops idle sessions anyway.
# MAGIC - **A cron job on a laptop** that calls the notebook. Until the laptop sleeps, or someone goes on holiday.
# MAGIC
# MAGIC What we want: something that starts the pipeline on a schedule, tries again when it fails, keeps the history of
# MAGIC every run, and tells someone when it stays red. That's a **job** (Lakeflow Jobs, in the menu **Jobs & Pipelines**).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The notebook the job runs
# MAGIC
# MAGIC Open `pipeline_job` in this folder. It's short:
# MAGIC
# MAGIC ```python
# MAGIC run_bronze_inline()
# MAGIC run_silver()
# MAGIC run_gold()
# MAGIC wrong = gold_diffs().where("diff_orders != 0 OR diff_revenue != 0")
# MAGIC if wrong.count() > 0:
# MAGIC     raise AssertionError("gold doesn't match a full recompute from silver")
# MAGIC dbutils.notebook.exit(json.dumps(summary))
# MAGIC ```
# MAGIC
# MAGIC Three things make it fit for a job:
# MAGIC
# MAGIC - every layer uses `availableNow`: read what's new, write it, stop. The job ends, the compute stops, nothing runs
# MAGIC   (and costs) in between
# MAGIC - every layer has its own checkpoint, so a run after 10 minutes or after a weekend does the right thing
# MAGIC - the check at the end turns a wrong gold into a **failed run**, instead of a wrong number on a dashboard
# MAGIC
# MAGIC One difference to the demos: bronze runs directly (`run_bronze_inline`), not through `_stream_once`. In a job a stop
# MAGIC on a new column *should* fail the task, so that the job's retry takes over.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Creating the job
# MAGIC
# MAGIC In the UI: **Jobs & Pipelines** → **Create** → **Job**, task type **Notebook**, path `pipeline_job`, compute
# MAGIC **Serverless**, and in the task settings **Retries** = 1. The exercise does it that way.
# MAGIC
# MAGIC Here we do the same with the Databricks SDK, so the demo runs by itself. Same settings, written down:

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

w = WorkspaceClient()
me = w.current_user.me().user_name
JOB_NAME = f"w10_pipeline_demo_{CATALOG}"
JOB_NOTEBOOK = f"{NOTEBOOK_ROOT}/08_production_job/pipeline_job"

for old in w.jobs.list(name=JOB_NAME):                 # start fresh when you run the demo again
    w.jobs.delete(old.job_id)

job = w.jobs.create(
    name=JOB_NAME,
    tasks=[jobs.Task(
        task_key="pipeline",
        notebook_task=jobs.NotebookTask(notebook_path=JOB_NOTEBOOK, base_parameters={"catalog": CATALOG}),
        max_retries=1,                                 # a new column stops bronze once; the retry carries on
        min_retry_interval_millis=10_000,
    )],
)
print(f"Job {job.job_id} created: {w.config.host}/jobs/{job.job_id}")


def run_job():
    # run_job_and_wait and run_summary are in _pipeline: start the job, wait, read the result
    result, attempts, output = run_summary(w, run_job_and_wait(w, job.job_id))
    print(f"{result} after {attempts} attempt(s): {output}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The job runs
# MAGIC
# MAGIC First run: nothing new arrived since notebook 05.

# COMMAND ----------

run_job()

# COMMAND ----------

# MAGIC %md
# MAGIC Every layer 0, the check passed. A run like this is cheap: a few seconds of compute to find out there's nothing
# MAGIC to do. (The numbers come from the tables themselves: new rows in bronze, and what the `MERGE`s in silver and gold
# MAGIC changed, from their history.) Now Sunday's file lands, batch 6, and the job runs again (on a schedule this would happen by itself):

# COMMAND ----------

write_batch(6)
run_job()

# COMMAND ----------

# MAGIC %md
# MAGIC 79 new events in bronze. Silver: 40 new orders, 39 orders updated to `delivered`. Gold: every restaurant got its
# MAGIC new orders added, so all six rows changed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. At night, a new field
# MAGIC
# MAGIC The app gets an update on Monday night: orders now have a `voucher_code`. Notebook 03: Auto Loader stops once and
# MAGIC writes the new schema. In a notebook you ran the cell again. In the job, the **retry** does it:

# COMMAND ----------

write_batch(7)
run_job()

# COMMAND ----------

# MAGIC %md
# MAGIC Two attempts: the first stopped on `voucher_code`, the retry loaded the 10 orders: 10 new rows in bronze, 10 new
# MAGIC orders in silver. Nobody had to get up. Open the
# MAGIC job in the UI (link above) and look at the run: you'll see the failed attempt and the retry.
# MAGIC
# MAGIC Silver doesn't have a `voucher_code` column, its `MERGE` selects its own columns. The new field is safe in bronze
# MAGIC until someone decides silver needs it. That's the job of bronze.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. A schedule, and a warning
# MAGIC
# MAGIC In the UI: **Schedules & Triggers** → **Add trigger** → **Scheduled**, every 10 minutes. Or with the SDK. We add it
# MAGIC **paused**, so it doesn't run while you read this:

# COMMAND ----------

w.jobs.update(job_id=job.job_id, new_settings=jobs.JobSettings(
    schedule=jobs.CronSchedule(quartz_cron_expression="0 0/10 * * * ?", timezone_id="Europe/Zurich",
                               pause_status=jobs.PauseStatus.PAUSED)))
print(w.jobs.get(job.job_id).settings.schedule)

# COMMAND ----------

# MAGIC %md
# MAGIC **Free Edition has a daily compute limit.** A job every 10 minutes is 144 runs a day, and they add up, even when
# MAGIC there's nothing to do. If you un-pause it to try, pause it again afterwards. `99_cleanup` deletes the jobs of this
# MAGIC week.
# MAGIC
# MAGIC For the record, the other things a job gives you, all in its settings:
# MAGIC
# MAGIC | Setting | What for |
# MAGIC |---|---|
# MAGIC | retries | the stop on a new column, short network problems |
# MAGIC | notifications | an email when a run fails, so «red» reaches a person |
# MAGIC | timeout | a run that hangs doesn't block the next ones |
# MAGIC | several tasks | bronze, silver, gold as tasks with dependencies, each restartable on its own (extended exercise) |
# MAGIC | run history | every run with its output, for weeks |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Under the hood
# MAGIC
# MAGIC Nothing new, and that's the point. The job starts the same three queries with the same checkpoints as your
# MAGIC notebooks did. Run them from a notebook in between, and the job simply finds less to do. What changes is who
# MAGIC presses *Run*:

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {BRONZE_TABLE}").select("version", "timestamp", "operation").orderBy("version"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What you saw, this week
# MAGIC
# MAGIC | Notebook | Question | Answer |
# MAGIC |---|---|---|
# MAGIC | 01 | every file exactly once? | Auto Loader, the list of files in the checkpoint |
# MAGIC | 02 | what is behind it? | Structured Streaming: a growing table, micro-batches, state, `availableNow` |
# MAGIC | 03 | a new column, a broken value? | schema evolution: stop once, retry; `_rescued_data` |
# MAGIC | 04 | one row per order? | Delta table as a source, `foreachBatch` + `MERGE` «only if newer» |
# MAGIC | 05 | Anna cancels? | Change Data Feed, plus and minus into gold |
# MAGIC | 06 | without files? | the Kafka source, offsets in the checkpoint |
# MAGIC | 07 | when is a window final? | the watermark |
# MAGIC | 08 | who runs it? | a job with retries and a check |
# MAGIC
# MAGIC Next step on Databricks, if you like: **Lakeflow Declarative Pipelines** take the checkpoints, the retries and the
# MAGIC `MERGE` logic off your hands. You'll recognise every idea of this week in them. First: `2_exercise_short_production_job`.
