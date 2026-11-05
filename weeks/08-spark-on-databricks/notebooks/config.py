"""Settings shared by all Week 8 notebooks (Spark on Databricks).

Change the catalog name here, once – every notebook in this folder reads it with
`import config`. After a change, re-run the notebook's first code cell: it reloads this file.
"""
import re

# Unity Catalog catalog for everything of week 8 (schemas nyc_taxi and playground)
CATALOG = "denghs26"

if not re.fullmatch(r"[a-z][a-z0-9_]*", CATALOG):
    raise ValueError(f"Invalid catalog name {CATALOG!r} in config.py – use lower-case letters, digits and underscores")
