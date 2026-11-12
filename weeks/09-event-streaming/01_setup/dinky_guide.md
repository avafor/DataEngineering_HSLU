# Dinky Guide

[Dinky](https://github.com/DataLinkDC/dinky) is a web IDE for Apache Flink. You write
SQL in the browser, it ships the job to the Flink cluster, and shows you
results.

Do this **once per Codespace, before exercise 07**. You only need Dinky for the **Flink track**
([../03_exercise/exercise_07_flink.md](../03_exercise/exercise_07_flink.md),
[08](../03_exercise/exercise_08_flink_food.md) and
[09](../03_exercise/exercise_09_flink_joins.md)).
The Kafka notebooks don't use it.

## Open Dinky

In VS Code, click the **PORTS** tab at the bottom → port `8888` → globe
icon (*Open in Browser*).

First time only: Dinky asks you to **set the admin password**. Choose your
own and note it down — you need it to log in again (user `admin`). Click
*Next* through any further welcome dialogs; you'll land on the Dinky home page.

> If the UI is in Chinese, open Dinky in an incognito tab — the
> container patches the language on first run, but a cached old session
> can override it.

## Step 1 — Register the Flink cluster (once per Codespace)

Dinky needs to know where to send your SQL jobs.

> ⚠️ **Watch the submenu name.** Under *Registration Center → Cluster*
> there are **two** options that look similar:
>
> | Submenu | Use it for | Use it here? |
> |---|---|---|
> | **Cluster Instance** (a.k.a. *Flink Instance*) | a long-running Flink session you connect to | ✅ **Yes** |
> | **Cluster Config** | Yarn / Kubernetes / Application-mode jobs (Hadoop config, Flink Lib Path, etc.) | ❌ no |
>
> If the dialog you see says *"Yarn (Pre-Job/Application)"* and asks for
> Hadoop config → you're in the wrong place. Go back and pick
> *Cluster Instance* instead.

1. Top menu: **Registration Center**
2. Side menu: **Cluster** → **Cluster Instance** (sometimes labelled
   *Flink Instance*)
3. Click **+ Add** (or *Create*) — a *Create Cluster Instance* dialog
   opens.
4. Fill in:

   | Field | Value |
   |---|---|
   | **Name** | `deng` (any short name) |
   | **Alias** | `deng` (or leave blank) |
   | **Type** | **`Standalone`** (not Yarn!) |
   | **JobManager HA Address** | `flink-jobmanager:8081` |
   | **Note** | leave empty |

   The address must be `flink-jobmanager:8081` — that's the Docker
   service name, *not* `localhost`. Dinky runs inside its own container
   and reaches Flink via the docker network. If the field rejects the
   value, prefix with `http://`.

5. Click **Save**.

The cluster status indicator should turn **green / Normal**. If it stays
red, double-check the JobManager URL — it must be `http://flink-jobmanager:8081`
(Docker DNS), *not* `localhost:8081`.

## Step 2 — Open Data Studio and create a task

The actual SQL editor is under **Data Studio** in the top menu.

1. **Data Studio** → click the **+** in the file tree. A fresh Dinky has
   no folder yet, so it first asks you to **create a root directory**:
   enter a folder name, e.g. `deng`, and click **OK**. Without a folder
   Dinky cannot create a task.
2. Only now create the task: **right-click the new folder** → **Create Task**.
3. Pick **FlinkSQL** as the type and give it any name (e.g. `scratch`).
4. In the right-hand panel set **two** things:
   - **Cluster Configuration / Flink Instance** → `deng` (the cluster
     instance you registered in Step 1).
   - **Catalog** → `DefaultCatalog`.

> ⚠️ **You must set BOTH every time you create a new task.** Without
> them, two failure modes hit you:
>
> | Missing setting | What goes wrong |
> |---|---|
> | **Catalog ≠ DefaultCatalog** | `CREATE TABLE` succeeds, but the table only lives inside *this* task. Open another task → `Object not found`. With `DefaultCatalog` the table is shared across all tasks of the Codespace. |
> | **Cluster Configuration not set** | Hitting *Execute* either does nothing or runs the SQL inside Dinky's tiny built-in mini-cluster, which has no Kafka connector. You'll see weird "connector kafka not found" errors. With `deng` selected your job ships to the proper Flink jobmanager that *does* have the connector. |
>
> Once set, each task remembers its choice — but new tasks always start
> blank. **Habituate the check** before you click *Execute*.

## Step 3 — Run something

Paste a tiny test:

```sql
SELECT 'hello from flink' AS msg;
```

Dinky has **two** run buttons in the toolbar above the editor — pick the
right one:

| Button | Use it for | Where the rows go |
|---|---|---|
| red 🐞 **Search** (debug, `Shift+F9`) | **`SELECT`** — when you want to *see* rows | the **Result** tab at the bottom |
| green ▶ **Execute** (`Shift+F10`) | **`INSERT INTO`** and `CREATE TABLE` — jobs that write somewhere | into the sink topic; nothing shows in Result |

Run the test with **🐞 Search**. After ~10 s the **Result** tab at the
bottom shows one row. For a streaming `SELECT` (e.g. on `bluesky_posts`)
the rows keep coming; if the tab does not refresh by itself, click
**Get the latest data**.

> The **Flink UI** (port `8081`) only shows *that* a job runs, not its
> rows. Its counters *Records Received/Sent* stay at `0` when source and
> sink are chained into one box — that is normal.

Stop a job with the red ■ button.

> Why "stop"? `SELECT` queries on streaming data run **forever** by
> default. They're not snapshots like in normal SQL — they're standing
> queries that keep producing rows. Each running job uses a Flink task
> slot, and there are only 4. **Stop jobs you no longer watch.**

## Step 4 — Work with persistent tables

Most exercises define a Kafka source table that lives across multiple
tasks. The pattern is always the same:

```sql
CREATE TABLE IF NOT EXISTS my_topic_table (
    -- ... columns ...
) WITH (
    'connector' = 'kafka',
    'topic' = 'my-topic',
    'properties.bootstrap.servers' = 'redpanda:29092',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
);
```

- Run this **once** (in any task) with **Catalog: DefaultCatalog**. The
  table now exists for the whole Codespace lifetime.
- Other tasks can `SELECT * FROM my_topic_table` directly — no need to
  re-create.
- If you ever need to change the schema: `DROP TABLE my_topic_table;`
  then re-create.

## Useful tabs in the Dinky UI

| Tab | What for |
|---|---|
| **Data Studio** | The SQL editor |
| **Result** (bottom of editor) | live rows from the job |
| **Job** | history of submitted jobs, click to inspect |
| **Registration Center → Cluster** | check that Flink is reachable |
| **Flink Web UI** (port 8081 in PORTS) | low-level: task slots, backpressure, checkpoints |

## Common pitfalls

| Symptom | Fix |
|---|---|
| `Object 'bluesky_posts' not found` | the table is in the wrong catalog. Set the task's catalog to **DefaultCatalog** and re-run the `CREATE TABLE`. |
| `No more slots available` | another job is hogging a slot. Go to the previous task → red ■. |
| Cluster status is **Abnormal** | JobManager URL wrong. It must be `http://flink-jobmanager:8081` (Docker DNS, not `localhost`). |
| `Result` tab shows nothing | the source topic is empty. Check Redpanda Console. For Bluesky: did you start the firehose? |
| You ran a `SELECT` and the Result tab stays empty | you used ▶ **Execute**: it submits the job but collects no rows. Stop it (■) and run the `SELECT` with 🐞 **Search**. If it is a streaming source: is data arriving (Redpanda Console)? |
| You hit **Execute** and nothing happens | look at the bottom-left status bar — if it says "submitting…" wait. Otherwise check that the task's *Cluster Configuration* is set. |
