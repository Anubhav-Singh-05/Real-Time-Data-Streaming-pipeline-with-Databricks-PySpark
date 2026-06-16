# Databricks notebook source
# MAGIC %md
# MAGIC ### Step 2 — Simulate a Producer 
# MAGIC Re-run this to simulate a producer. NOTE: Should be run manually each time

# COMMAND ----------

import json
import random
import uuid

from datetime import datetime, timezone

cities = [
    "Pune",
    "Mumbai",
    "Delhi",
    "Bangalore",
    "Hyderabad"
]

ride_types = [
    "Bike",
    "Auto",
    "Mini",
    "Sedan"
]


def generate_rides(n=20):

    return [
        {
            "ride_id": str(uuid.uuid4()),
            "event_time": datetime.now(timezone.utc).isoformat(),
            "city": random.choice(cities),
            "ride_type": random.choice(ride_types),
            "fare": round(random.uniform(50, 600), 2),
            "distance_km": round(random.uniform(1, 25), 2)
        }
        for _ in range(n)
    ]


batch_id = str(uuid.uuid4())[:8]

file_path = (
    f"/Volumes/streaming_demo/ride_booking/raw_rides/"
    f"batch_{batch_id}.json"
)

rides = generate_rides(20)

json_lines = "\n".join(
    json.dumps(r) for r in rides
)

dbutils.fs.put(
    file_path,
    json_lines,
    overwrite=True
)

print(
    f"Created {len(rides)} ride events"
)
print(
    f"File: {file_path}"
)
