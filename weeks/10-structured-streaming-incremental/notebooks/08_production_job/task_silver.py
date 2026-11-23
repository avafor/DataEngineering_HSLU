# Databricks notebook source
# MAGIC %md
# MAGIC # Task: silver
# MAGIC
# MAGIC One task of the three-task job in `3_exercise_extended_production_job`. It runs only the silver step.

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

# MAGIC %run ../_generator

# COMMAND ----------

# MAGIC %run ../_pipeline

# COMMAND ----------

rows = run_silver()
dbutils.notebook.exit(str(rows))
