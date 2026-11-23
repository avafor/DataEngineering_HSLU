# Databricks notebook source
# MAGIC %md
# MAGIC # Task: gold
# MAGIC
# MAGIC One task of the three-task job in `3_exercise_extended_production_job`. It runs only the gold step.

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

# MAGIC %run ../_generator

# COMMAND ----------

# MAGIC %run ../_pipeline

# COMMAND ----------

rows = run_gold()
dbutils.notebook.exit(str(rows))
