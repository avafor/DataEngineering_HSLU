# Databricks notebook source
# MAGIC %md
# MAGIC # 08 Who runs it? (exercise, extended)
# MAGIC
# MAGIC For at home. A job that is more than one notebook, a job that tells you when it fails, and a job that fails
# MAGIC on purpose.
# MAGIC
# MAGIC | Part | Question |
# MAGIC |---|---|
# MAGIC | 1 | Bronze, silver and gold as separate tasks: what for? |
# MAGIC | 2 | Who finds out at night that the job is red? |
# MAGIC | 3 | What does the check at the end of `pipeline_job` actually protect? |

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
me = w.current_user.me().user_name
FOLDER = f"{NOTEBOOK_ROOT}/08_production_job"

if not spark.catalog.tableExists(GOLD_TABLE):
    catch_up(5)


def fresh_job(name, **settings):
    # Delete jobs with this name, create a new one
    for old in w.jobs.list(name=name):
        w.jobs.delete(old.job_id)
    return w.jobs.create(name=name, **settings).job_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Three tasks instead of one
# MAGIC
# MAGIC ### What goes wrong
# MAGIC
# MAGIC `pipeline_job` does everything in one notebook. If gold fails, the retry runs bronze and silver again (cheap here,
# MAGIC they find nothing new, but in general wasteful), and the run history only says «the pipeline failed».
# MAGIC
# MAGIC ### How to do it properly
# MAGIC
# MAGIC One task per layer, with dependencies: silver starts when bronze is done, gold when silver is done. A failed task
# MAGIC can be repaired on its own (**Repair run** in the UI), and the history shows which layer it was. The notebooks
# MAGIC `task_bronze`, `task_silver` and `task_gold` in this folder each run one layer.
# MAGIC
# MAGIC ### Task 8.4 – A three-task job ★★
# MAGIC
# MAGIC Create the job `w10_pipeline_three_tasks` with three notebook tasks `bronze`, `silver`, `gold` (notebooks
# MAGIC `f"{FOLDER}/task_bronze"` etc., parameter `catalog`), silver depending on bronze and gold on silver
# MAGIC (`depends_on=[jobs.TaskDependency(task_key="bronze")]`), bronze with 1 retry. Run it and wait. Store the job id in
# MAGIC `three_id` and the finished run in `three_run`.

# COMMAND ----------

# TODO Task 8.4
raise NotImplementedError("Task 8.4")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>A small function that builds a <code>jobs.Task</code> for one layer saves typing. <code>depends_on</code> takes a list of <code>jobs.TaskDependency(task_key=...)</code>. <code>fresh_job(name, tasks=[...])</code> returns the job id.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>def task(name, after=None, retries=0):
# MAGIC     return jobs.Task(task_key=name,
# MAGIC         notebook_task=jobs.NotebookTask(notebook_path=f&quot;{FOLDER}/task_{name}&quot;, base_parameters={&quot;catalog&quot;: CATALOG}),
# MAGIC         depends_on=[jobs.TaskDependency(task_key=after)] if after else None, max_retries=retries)
# MAGIC three_id = fresh_job(&quot;w10_pipeline_three_tasks&quot;, tasks=[task(&quot;bronze&quot;, retries=1), task(&quot;silver&quot;, after=&quot;___&quot;), ...])</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>def task(name, after=None, retries=0):
# MAGIC     return jobs.Task(task_key=name,
# MAGIC                      notebook_task=jobs.NotebookTask(notebook_path=f&quot;{FOLDER}/task_{name}&quot;, base_parameters={&quot;catalog&quot;: CATALOG}),
# MAGIC                      depends_on=[jobs.TaskDependency(task_key=after)] if after else None,
# MAGIC                      max_retries=retries)
# MAGIC 
# MAGIC 
# MAGIC three_id = fresh_job(&quot;w10_pipeline_three_tasks&quot;,
# MAGIC                      tasks=[task(&quot;bronze&quot;, retries=1), task(&quot;silver&quot;, after=&quot;bronze&quot;), task(&quot;gold&quot;, after=&quot;silver&quot;)])
# MAGIC three_run = run_job_and_wait(w, three_id)</code></pre></details>

# COMMAND ----------

states = {t.task_key: t.state.result_state.value for t in three_run.tasks}
print("Correct!" if states == {"bronze": "SUCCESS", "silver": "SUCCESS", "gold": "SUCCESS"} else f"Not yet – {states}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Somebody has to know
# MAGIC
# MAGIC ### What goes wrong
# MAGIC
# MAGIC A job turns red at 2 a.m. and stays red. The run history knows. Nobody looks at it until a dashboard is wrong.
# MAGIC
# MAGIC ### How to do it properly
# MAGIC
# MAGIC Notifications: an email when a run fails (and, if you like, when it takes too long). In the UI under **Job
# MAGIC notifications**. And a schedule, which is what makes it a pipeline that runs by itself.
# MAGIC
# MAGIC ### Task 8.5 – Notification and schedule ★
# MAGIC
# MAGIC Update the job from Task 8.4: an email to yourself (`me`) on failure, and a **paused** schedule every 10 minutes
# MAGIC (`quartz_cron_expression="0 0/10 * * * ?"`, `timezone_id="Europe/Zurich"`). Use `w.jobs.update(job_id=...,
# MAGIC new_settings=jobs.JobSettings(...))`.

# COMMAND ----------

# TODO Task 8.5
raise NotImplementedError("Task 8.5")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>jobs.JobSettings</code> with two fields: <code>email_notifications=jobs.JobEmailNotifications(on_failure=[me])</code> and <code>schedule=jobs.CronSchedule(..., pause_status=jobs.PauseStatus.PAUSED)</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>w.jobs.update(job_id=three_id, new_settings=jobs.JobSettings(
# MAGIC     email_notifications=jobs.JobEmailNotifications(on_failure=[___]),
# MAGIC     schedule=jobs.CronSchedule(quartz_cron_expression=&quot;0 0/10 * * * ?&quot;, timezone_id=&quot;Europe/Zurich&quot;,
# MAGIC                                pause_status=jobs.PauseStatus.___)))</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>w.jobs.update(job_id=three_id, new_settings=jobs.JobSettings(
# MAGIC     email_notifications=jobs.JobEmailNotifications(on_failure=[me]),
# MAGIC     schedule=jobs.CronSchedule(quartz_cron_expression=&quot;0 0/10 * * * ?&quot;, timezone_id=&quot;Europe/Zurich&quot;,
# MAGIC                                pause_status=jobs.PauseStatus.PAUSED)))</code></pre></details>

# COMMAND ----------

s = w.jobs.get(three_id).settings
ok = (s.email_notifications and me in (s.email_notifications.on_failure or [])
      and s.schedule and s.schedule.pause_status == jobs.PauseStatus.PAUSED)
print("Correct!" if ok else "Not yet – the job needs an email on failure to you and a paused schedule")

# COMMAND ----------

# MAGIC %md
# MAGIC Keep it paused. Free Edition has a daily compute limit, and 144 runs a day use it up.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The check at the end
# MAGIC
# MAGIC ### What goes wrong
# MAGIC
# MAGIC Someone «fixes» a number in gold by hand. Or a replay adds a batch twice (notebook 05, extended). The pipeline
# MAGIC keeps running, green, and gold stays wrong from then on, because every run only adds changes to it.
# MAGIC
# MAGIC ### Why
# MAGIC
# MAGIC An incremental gold never looks at all of silver again. A mistake in it doesn't heal by itself.
# MAGIC
# MAGIC ### How to do it properly
# MAGIC
# MAGIC Check it. `pipeline_job` compares gold with a full recompute on every run and fails when they differ. A full
# MAGIC recompute is expensive, so in real life you'd do it once a night, or on a sample. And when it fails: rebuild gold
# MAGIC from silver once, then let the incremental runs carry on.
# MAGIC
# MAGIC ### Task 8.6 – Break it, see it, repair it ★★★
# MAGIC
# MAGIC 1. Create a job `w10_pipeline_checked` for `pipeline_job` **without** retries, store its id in `checked_id`.
# MAGIC 2. Change gold by hand: add 1 to `active_orders` of `Pizzeria Bella` (`UPDATE`). Run the job, store the result in
# MAGIC    `broken_result` (`run_summary(w, run)[0]`).
# MAGIC 3. Repair: overwrite gold with a full recompute from silver (the `full` query of `gold_diffs` in `_pipeline` is a
# MAGIC    good start; keep the columns `restaurant`, `active_orders`, `revenue_chf`, `updated_at`). Run the job again and
# MAGIC    store the result in `repaired_result`.

# COMMAND ----------

# TODO Task 8.6
raise NotImplementedError("Task 8.6")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Three steps: <code>fresh_job</code> without <code>max_retries</code>; an <code>UPDATE</code> on gold and <code>run_summary(w, run_job_and_wait(...))[0]</code>; an <code>INSERT OVERWRITE</code> of gold from a <code>GROUP BY restaurant</code> over silver, then run again.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>spark.sql(f&quot;UPDATE {GOLD_TABLE} SET active_orders = active_orders + 1 WHERE restaurant = &#x27;Pizzeria Bella&#x27;&quot;)
# MAGIC broken_result = run_summary(w, run_job_and_wait(w, checked_id))[0]
# MAGIC spark.sql(f&quot;INSERT OVERWRITE {GOLD_TABLE} SELECT restaurant, count_if(status != &#x27;cancelled&#x27;), ..., current_timestamp() FROM {SILVER_TABLE} GROUP BY restaurant&quot;)</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>checked_id = fresh_job(&quot;w10_pipeline_checked&quot;, tasks=[jobs.Task(
# MAGIC     task_key=&quot;pipeline&quot;,
# MAGIC     notebook_task=jobs.NotebookTask(notebook_path=f&quot;{FOLDER}/pipeline_job&quot;, base_parameters={&quot;catalog&quot;: CATALOG}))])
# MAGIC 
# MAGIC spark.sql(f&quot;UPDATE {GOLD_TABLE} SET active_orders = active_orders + 1 WHERE restaurant = &#x27;Pizzeria Bella&#x27;&quot;)
# MAGIC broken_result = run_summary(w, run_job_and_wait(w, checked_id))[0]
# MAGIC 
# MAGIC spark.sql(f&quot;&quot;&quot;
# MAGIC     INSERT OVERWRITE {GOLD_TABLE}
# MAGIC     SELECT restaurant,
# MAGIC            count_if(status != &#x27;cancelled&#x27;) AS active_orders,
# MAGIC            round(sum(CASE WHEN status != &#x27;cancelled&#x27; THEN amount_chf ELSE 0 END), 2) AS revenue_chf,
# MAGIC            current_timestamp() AS updated_at
# MAGIC     FROM {SILVER_TABLE}
# MAGIC     GROUP BY restaurant
# MAGIC &quot;&quot;&quot;)
# MAGIC repaired_result = run_summary(w, run_job_and_wait(w, checked_id))[0]
# MAGIC print(broken_result, repaired_result)</code></pre></details>

# COMMAND ----------

ok = broken_result == "FAILED" and repaired_result == "SUCCESS"
print("Correct!" if ok else f"Not yet – expected FAILED, then SUCCESS, got {broken_result} and {repaired_result}")

# COMMAND ----------

# MAGIC %md
# MAGIC Why does the repair work with the gold checkpoint untouched? Gold's bookmark is a version of silver. After the
# MAGIC recompute, gold matches silver as it is *now*, and the next run adds only the changes after the bookmark, which
# MAGIC aren't in the recompute yet. (If silver had changed between the last gold run and your recompute, those changes
# MAGIC would be counted twice. In a real repair you pause the job first.)
# MAGIC
# MAGIC ## Clean up (optional)
# MAGIC
# MAGIC Deletes the two jobs of this notebook. `99_cleanup` deletes all jobs of this week.

# COMMAND ----------

for name in ["w10_pipeline_three_tasks", "w10_pipeline_checked"]:
    for old in w.jobs.list(name=name):
        w.jobs.delete(old.job_id)
