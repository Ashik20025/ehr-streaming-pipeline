# Real-Time Streaming EHR Ingestion and Validation Pipeline

This repository contains a local streaming systems project inspired by
"Real-Time Data Ingestion Pipelines for Streaming EHR Systems" by Andrew
Psaltis and Olivia Stone. The project implements an end-to-end pipeline for
synthetic Electronic Health Record (EHR) events using Kafka, Flink, Redis,
Cassandra, and a React/Material UI dashboard.

The implementation is designed to run on a laptop with Docker Desktop. It does
not use cloud services or real patient data.

## Architecture

```text
Synthetic EHR Producer -> Kafka -> Flink -> Redis/Cassandra -> Report/Dashboard
```

The distributed pipeline uses:

- Kafka for streaming ingestion
- Flink for validation, duplicate detection, event-time handling, enrichment,
  quarantine routing, and alert generation
- Redis for low-latency synthetic patient metadata lookup
- Cassandra for valid events, quarantined records, and generated alerts
- React and Material UI for dashboard visualization

## Features

- Synthetic EHR event generation for vitals, labs, medication orders, and
  admissions
- Controlled injection of invalid records, duplicates, late events, and
  out-of-order events
- Required-field, event-type, timestamp, schema, PHI-like field, and clinical
  range validation
- Duplicate detection using Flink keyed state
- Event-time processing with an allowed lateness window
- Invalid-record quarantine with reason counts
- Redis-based patient metadata enrichment
- Cassandra storage for accepted records, quarantined records, and alerts
- Throughput and latency metrics
- HTML report and React/Material UI dashboard output

## Repository Layout

```text
docker-compose.yml                         Local distributed services
cassandra/schema.cql                       Cassandra keyspace and tables
flink-job/                                 Java Flink streaming job
stream-tools/producer.py                   Kafka synthetic EHR producer
stream-tools/report.py                     Cassandra summary/report generator
dashboard/                                 React and Material UI dashboard
scripts/run_distributed_pipeline.sh        Full distributed pipeline runner
scripts/run_experiment_matrix.sh           Multi-run experiment script
src/                                       Dependency-free local prototype
tests/                                     Validation tests
artifacts/                                 Generated preliminary outputs
```

## Run the Distributed Pipeline

Start Docker Desktop, then run:

```bash
chmod +x scripts/run_distributed_pipeline.sh
EVENTS=5000 PATIENTS=1000 RATE=0 ALLOWED_LATENESS=300 ./scripts/run_distributed_pipeline.sh
```

The script builds and runs the local Kafka/Flink/Redis/Cassandra pipeline,
generates synthetic EHR events, writes processed records to Cassandra, and
creates summary outputs under `artifacts/`.

Generated outputs include:

```text
artifacts/distributed_report.html
artifacts/distributed_summary.json
artifacts/distributed_run_metrics.json
```

## Dashboards

Flink dashboard:

```text
http://localhost:8081
```

React/Material UI dashboard:

```bash
docker compose up --build dashboard
```

Then open:

```text
http://localhost:5173
```

Preliminary-output view:

```text
http://localhost:5173/?view=preliminary
```

## Local Prototype

A smaller dependency-free prototype is included for fast validation of the core
pipeline logic:

```bash
python3 src/run_pipeline.py --events 20000 --seed 247
open artifacts/report.html
```

Useful SQLite checks for the local prototype:

```bash
sqlite3 data/ehr_pipeline.db "select count(*) from events;"
sqlite3 data/ehr_pipeline.db "select reason, count(*) from quarantine group by reason;"
sqlite3 data/ehr_pipeline.db "select count(*) from alerts;"
```

## Preliminary Run

One preliminary local distributed run processed 5,000 synthetic EHR events:

- 4,689 valid records
- 311 quarantined records
- 5 possible-sepsis alerts
- 27,375 producer events/sec
- 3,210 ms average ingestion latency
- 5,427 ms p95 ingestion latency

The quarantined records are expected because the synthetic generator
intentionally injects invalid and problematic events to test validation,
duplicate detection, late-event handling, and quarantine routing.

## Experiment Matrix

The experiment script runs multiple workload configurations and summarizes the
results:

```bash
chmod +x scripts/run_experiment_matrix.sh
EVENTS_LIST="5000 10000" RATES_LIST="1000 0" PATIENTS=1000 ./scripts/run_experiment_matrix.sh
```

Experiment outputs are written under `artifacts/experiments/`:

```text
artifacts/experiments/experiment_summary.csv
artifacts/experiments/experiment_summary.md
artifacts/experiments/experiment_summary.html
```

## Notes

All generated data is synthetic. The project does not contain real EHR data or
personally identifiable patient information.
