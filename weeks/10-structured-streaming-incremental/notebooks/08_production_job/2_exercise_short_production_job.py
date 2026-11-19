# Databricks notebook source
# MAGIC %md
# MAGIC # 08 Who runs it? (exercise, short)
# MAGIC
# MAGIC About 10 minutes. You create the job yourself, **in the UI**, run it, and read what it did. The cells after each
# MAGIC task check your job through the Databricks SDK.

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

# MAGIC %run ../_generator

# COMMAND ----------

# MAGIC %run ../_pipeline

# COMMAND ----------

import json

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

w = WorkspaceClient()
EX_JOB = "w10_pipeline_exercise"
JOB_NOTEBOOK = f"{NOTEBOOK_ROOT}/08_production_job/pipeline_job"

if not spark.catalog.tableExists(GOLD_TABLE):
    catch_up(5)

print("Name for your job:   ", EX_JOB)
print("Notebook of the task:", JOB_NOTEBOOK)


def my_job():
    found = list(w.jobs.list(name=EX_JOB))
    return w.jobs.get(found[0].job_id) if found else None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 8.1 – Create the job ★
# MAGIC
# MAGIC In the left menu: **Jobs & Pipelines** → **Create** → **Job**.
# MAGIC
# MAGIC 1. Job name (top left): the `EX_JOB` printed above
# MAGIC 2. Task name `pipeline`, type **Notebook**, source **Workspace**, path: the notebook printed above
# MAGIC 3. Compute: **Serverless**
# MAGIC 4. Parameters: add `catalog` with your catalog (normally `denghs26`)
# MAGIC 5. Task settings: **Retries** → 1
# MAGIC 6. **Create task**
# MAGIC
# MAGIC Then run the check. (The solution below does the same with the SDK, in case the UI looks different for you.)

# COMMAND ----------

# TODO Task 8.1
raise NotImplementedError("Task 8.1")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>In the UI, the path of the notebook is printed in the first cell. Retries are in the task settings, not in the job settings. The parameter <code>catalog</code> goes under the task&#x27;s parameters.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>w.jobs.create(name=EX_JOB, tasks=[jobs.Task(task_key=&quot;pipeline&quot;,
# MAGIC     notebook_task=jobs.NotebookTask(notebook_path=JOB_NOTEBOOK, base_parameters={&quot;catalog&quot;: CATALOG}),
# MAGIC     max_retries=___)])</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>for old in w.jobs.list(name=EX_JOB):
# MAGIC     w.jobs.delete(old.job_id)
# MAGIC w.jobs.create(name=EX_JOB, tasks=[jobs.Task(
# MAGIC     task_key=&quot;pipeline&quot;,
# MAGIC     notebook_task=jobs.NotebookTask(notebook_path=JOB_NOTEBOOK, base_parameters={&quot;catalog&quot;: CATALOG}),
# MAGIC     max_retries=1)])</code></pre></details>

# COMMAND ----------

j = my_job()
task = j.settings.tasks[0] if j else None
ok = task is not None and task.notebook_task.notebook_path.endswith("08_production_job/pipeline_job") and task.max_retries == 1
print("Correct!" if ok else f"Not yet – no job {EX_JOB} with the notebook pipeline_job and 1 retry found")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 8.2 – Run it ★
# MAGIC
# MAGIC Click **Run now** in the job and wait until the run is green. Or call `run_job_and_wait(w, job_id)` from `_pipeline`.
# MAGIC Then store the latest run of your job in `last_run`: `w.jobs.list_runs(job_id=..., limit=1)` gives you its id,
# MAGIC `w.jobs.get_run(run_id)` the run.

# COMMAND ----------

# TODO Task 8.2
raise NotImplementedError("Task 8.2")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>run_job_and_wait(w, my_job().job_id)</code> starts and waits. <code>w.jobs.list_runs(job_id=..., limit=1)</code> returns the newest run first.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>latest = next(iter(w.jobs.list_runs(job_id=my_job().job_id, limit=1)))
# MAGIC last_run = w.jobs.get_run(latest.___)</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>run_job_and_wait(w, my_job().job_id)
# MAGIC latest = next(iter(w.jobs.list_runs(job_id=my_job().job_id, limit=1)))
# MAGIC last_run = w.jobs.get_run(latest.run_id)</code></pre></details>

# COMMAND ----------

result = last_run.state.result_state.value if last_run.state.result_state else last_run.state.life_cycle_state.value
print("Correct!" if result == "SUCCESS" else f"Not yet – the latest run is {result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 8.3 – What did it do? ★★
# MAGIC
# MAGIC In the UI: open the run, then the task, the output is at the bottom. With the SDK: `run_summary(w, last_run)` from
# MAGIC `_pipeline` returns the result, the number of attempts and the output. The output is JSON. Store it as a dict in
# MAGIC `summary`.
# MAGIC
# MAGIC Why are the numbers 0?

# COMMAND ----------

# TODO Task 8.3
raise NotImplementedError("Task 8.3")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>run_summary(w, last_run)</code> returns three values; the third is the notebook output, a JSON string.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>_, _, output = run_summary(w, last_run)
# MAGIC summary = json.___(output)</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>_, _, output = run_summary(w, last_run)
# MAGIC summary = json.loads(output)
# MAGIC print(summary)</code></pre></details>

# COMMAND ----------

ok = summary.get("check") == "gold matches silver" and "bronze_new_rows" in summary
print("Correct!" if ok else f"Not yet – expected bronze_new_rows and the check in {summary}")

# COMMAND ----------

# MAGIC %md
# MAGIC 0 everywhere, because no new file arrived since the last run. Put a file in with `write_batch(6)` and run the job
# MAGIC again if you want to see numbers (if the demo already sent batch 6, nothing changes: the file is there already).
# MAGIC
# MAGIC Done. More in `3_exercise_extended_production_job`. And when you're finished with the week: `99_cleanup` deletes
# MAGIC the jobs again.
