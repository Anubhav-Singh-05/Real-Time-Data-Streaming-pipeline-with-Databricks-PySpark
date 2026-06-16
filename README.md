# Real-Time Data Streaming Pipeline with Databricks & PySpark

## Project Overview

This project demonstrates an end-to-end real-time data streaming pipeline built using Databricks, PySpark Structured Streaming, Auto Loader, Delta Lake, and Unity Catalog.

The pipeline simulates ride booking events, continuously ingests JSON files, performs streaming aggregations, and stores analytics-ready data in Delta tables.

---

## Architecture

Ride Events Generator
↓
JSON Files
↓
Databricks Volume
↓
Auto Loader (cloudFiles)
↓
PySpark Structured Streaming
↓
Window Aggregations
↓
Delta Lake Table
↓
SQL Analytics

---

## Tech Stack

- Databricks Free Edition
- PySpark
- Structured Streaming
- Auto Loader (cloudFiles)
- Delta Lake
- Unity Catalog
- SQL
- Python

---

## Project Workflow

### Step 1: Environment Setup

- Create Catalog
- Create Schema
- Create Volumes
- Configure storage locations

Notebook:

```text
00_databricks_catalog_setup.py
```

### Step 2: Generate Streaming Data

Simulate ride booking events with:

- Ride ID
- City
- Ride Type
- Fare
- Distance
- Event Timestamp

Notebook:

```text
01_streaming_data_generator.py
```

### Step 3: Stream Processing

- Auto Loader monitors incoming JSON files
- Reads new files incrementally
- Applies schema inference
- Performs windowed aggregations

Notebook:

```text
02_spark_structured_streaming_pipeline.py
```

---

## Sample Aggregation

Metrics calculated:

- Total Fare per City
- Ride Count per City
- Window Based Analytics

Example Output:

| City | Total Fare | Ride Count |
|--------|------------|------------|
| Bangalore | 2101.96 | 6 |
| Hyderabad | 2014.47 | 6 |
| Delhi | 782.79 | 3 |
| Mumbai | 760.64 | 3 |
| Pune | 472.33 | 2 |

---

## Key Features

- Real-time file ingestion
- Incremental processing
- Exactly-once semantics
- Checkpointing support
- Delta Lake storage
- Window aggregations
- Streaming analytics

---

## Learning Outcomes

- Databricks Unity Catalog
- Databricks Volumes
- Auto Loader
- Structured Streaming
- Delta Lake
- Checkpoint Management
- Streaming ETL Pipelines

---

## Repository Structure

```text
.
├── notebooks
│   ├── 00_databricks_catalog_setup.py
│   ├── 01_streaming_data_generator.py
│   ├── 02_spark_structured_streaming_pipeline.py
│   └── README.md
│
├── README.md
└── .gitignore
```

---

## Author

Anubhav Singh

LinkedIn:
https://linkedin.com/in/anubhavsinghsomvanshi

GitHub:
https://github.com/Anubhav-Singh-05
