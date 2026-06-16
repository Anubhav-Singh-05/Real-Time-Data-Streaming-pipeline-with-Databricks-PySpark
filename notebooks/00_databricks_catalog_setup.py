# Databricks notebook source
# MAGIC %md
# MAGIC ### Step 1 — Create a Catalog/Schema/Volume for streaming

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS streaming_demo;
# MAGIC USE CATALOG streaming_demo;
# MAGIC CREATE SCHEMA IF NOT EXISTS ride_booking;
# MAGIC USE SCHEMA ride_booking;
# MAGIC
# MAGIC -- Landing zone for incoming JSON files
# MAGIC CREATE VOLUME IF NOT EXISTS raw_rides;
# MAGIC
# MAGIC -- Checkpoint location for streaming state
# MAGIC CREATE VOLUME IF NOT EXISTS checkpoints;
