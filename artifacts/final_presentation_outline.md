# Final Presentation Outline

## 1. Motivation

- Healthcare systems increasingly receive EHR data as continuous streams.
- Streaming ingestion needs validation, duplicate handling, late-event handling,
  quarantine routing, and observable performance metrics.
- This project implements a local version of that pipeline using synthetic data.

## 2. Paper Connection

- The project is inspired by "Real-Time Data Ingestion Pipelines for Streaming
  EHR Systems" by Andrew Psaltis and Olivia Stone.
- The implementation follows the paper's systems ideas: ingestion layer, stream
  processing, validation, scalable storage, and monitoring/reporting.

## 3. Architecture

```text
Synthetic EHR Producer -> Kafka -> Flink -> Redis/Cassandra -> Dashboard
```

- Kafka receives raw synthetic EHR events.
- Flink validates, deduplicates, handles event time, enriches, quarantines, and
  generates alerts.
- Redis stores synthetic patient metadata used during stream enrichment.
- Cassandra stores valid records, quarantined records, and alerts.
- The dashboard visualizes counts, throughput, latency, event types, and error
  reasons.

## 4. Live Presentation Assets

- React and Material UI dashboard at `http://localhost:5173`
- Flink runtime UI at `http://localhost:8081`
- Saved experiment summary and generated report as presentation fallback

## 5. Implementation Details

- Synthetic records include vitals, labs, medications, and admissions.
- The generator intentionally injects duplicates, invalid records, late events,
  and out-of-order events.
- Flink uses keyed state for duplicate detection.
- Bad records are routed to quarantine rather than silently dropped.
- Possible-sepsis alerts are generated from a simple stateful rule.

## 6. Current Result Snapshot

Latest stored distributed run:

- Events sent: 10,000
- Synthetic patients: 5,000
- Valid records: 9,425
- Quarantined records: 575
- Accepted records: 94.25%
- Producer throughput: 33,783 events/sec
- Average latency: 5,459 ms
- P95 latency: 9,826 ms

## 7. Final Experiment Plan

Final experiment matrix already completed:

```bash
EVENTS_LIST="5000 10000" RATES_LIST="1000 0" PATIENTS=5000 ALLOWED_LATENESS=300 ./scripts/run_experiment_matrix.sh
```

Then use:

```bash
open artifacts/experiments/experiment_summary.html
open artifacts/final_report_draft.md
docker compose up --build dashboard
```

## 8. Discussion Points

- Why invalid records are expected: they are injected to test validation.
- Why Redis is useful: fast metadata enrichment during stream processing.
- Why Cassandra is useful: write-heavy, queryable serving store for streaming
  outputs.
- Why unlimited-rate runs have higher latency: the producer can outrun
  downstream processing and Cassandra writes.

## 9. Limitations

- Synthetic data only; no real patient records.
- Single-machine Docker deployment rather than a real cluster.
- Basic alert logic, not a clinically validated decision-support model.

## 10. Future Work

- Add Synthea/FHIR synthetic records.
- Add failure-recovery tests by restarting Kafka/Flink/Cassandra.
- Compare Flink parallelism and Kafka partition counts.
- Add richer clinical alert rules and dashboard drilldowns.
