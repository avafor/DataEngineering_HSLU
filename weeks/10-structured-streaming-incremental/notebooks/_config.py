# Databricks notebook source
# MAGIC %md
# MAGIC # Config
# MAGIC
# MAGIC Paths and table names for all notebooks of this week. The other notebooks pull it in with `%run`,
# MAGIC you don't have to run it yourself.
# MAGIC
# MAGIC The catalog is `denghs26`, the same as in week 8. `00_setup` creates it if it's missing.

# COMMAND ----------

dbutils.widgets.text("catalog", "denghs26", "Catalog")
CATALOG = dbutils.widgets.get("catalog")

# The pipeline: the app drops JSON files into the landing folder, and we move them on from there
LANDING_VOLUME = f"/Volumes/{CATALOG}/landing/orders_raw"
INCOMING_DIR = f"{LANDING_VOLUME}/incoming"

# Checkpoints and schema locations have to be on a volume when you run on serverless
STATE_VOLUME = f"/Volumes/{CATALOG}/landing/stream_state"
BRONZE_SCHEMA_LOCATION = f"{STATE_VOLUME}/bronze_orders/_schema"
BRONZE_CHECKPOINT = f"{STATE_VOLUME}/bronze_orders/_checkpoint"
SILVER_CHECKPOINT = f"{STATE_VOLUME}/silver_orders/_checkpoint"
GOLD_CHECKPOINT = f"{STATE_VOLUME}/gold_revenue/_checkpoint"

BRONZE_TABLE = f"{CATALOG}.bronze.orders"
SILVER_TABLE = f"{CATALOG}.silver.orders"
GOLD_TABLE = f"{CATALOG}.gold.restaurant_revenue"

# The playground: for trying things out and for the exercises, so the pipeline above stays clean.
# Every notebook uses its own sub-folder and its own table prefix there.
PLAYGROUND = f"{CATALOG}.playground"
PRACTICE_VOLUME = f"/Volumes/{CATALOG}/playground/w10_practice"


def _report(progress):
    # One progress report as a dict. Read the JSON: in a job the object's own fields can be empty
    import json as _json
    if isinstance(progress, dict):
        return progress
    try:
        return _json.loads(progress.json)
    except Exception:
        return {"numInputRows": getattr(progress, "numInputRows", 0)}


def batch_sizes(query):
    # Rows read per micro-batch of a finished query (recentProgress has one entry per micro-batch)
    return [_report(p).get("numInputRows") or 0 for p in query.recentProgress]


def input_rows(query):
    # All rows a query read in this run
    return sum(batch_sizes(query))


def progress_reports(query):
    # All progress reports of a finished query, one per micro-batch, as dicts
    return [_report(p) for p in query.recentProgress]


def last_progress(query):
    # The progress report of the last micro-batch that read rows, as a dict
    reports = progress_reports(query)
    with_rows = [p for p in reports if (p.get("numInputRows") or 0) > 0]
    return (with_rows or reports or [{}])[-1]


import json as _json
import re as _re


def _notebook_root():
    # The folder with _config, _stream_once etc., seen from the notebook that is running
    path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    folder = path.rsplit("/", 1)[0]
    if _re.match(r"^\d\d_", folder.rsplit("/", 1)[-1]):
        folder = folder.rsplit("/", 1)[0]
    return folder


NOTEBOOK_ROOT = _notebook_root()


def run_stream_once(target_table, state, folder=None, source_table=None, **options):
    """Run one streaming query with availableNow in a notebook run of its own (see _stream_once), return the rows read.

    folder: Auto Loader on this folder, or source_table: a stream on this Delta table. Extra reader options as keyword
    arguments, names with a dot as a dict: **{"cloudFiles.schemaEvolutionMode": "rescue"}.
    If the stream fails, this raises a RuntimeError with the reason.
    """
    args = {"target_table": target_table, "state": state, "folder": folder or "",
            "source_table": source_table or "", "options": _json.dumps(options)}
    try:
        return int(dbutils.notebook.run(f"{NOTEBOOK_ROOT}/_stream_once", 900, args))
    except Exception:
        try:
            message = dbutils.fs.head(f"{state}/_last_error.txt", 20_000)
        except Exception:
            message = "the stream failed, see the run of _stream_once for details"
        # Show the actual reason, not the wrapper around it ([STREAM_FAILED] Query ... terminated with exception: ...)
        classes = [m for m in _re.finditer(r"\[[A-Z][A-Z_.]+\]", message) if m.group() != "[STREAM_FAILED]"]
        raise RuntimeError(message[classes[0].start():][:500] if classes else message[:500]) from None


print(f"Catalog     : {CATALOG}")
print(f"Incoming    : {INCOMING_DIR}")
print(f"Bronze      : {BRONZE_TABLE}")
print(f"Playground  : {PLAYGROUND}, files in {PRACTICE_VOLUME}")
