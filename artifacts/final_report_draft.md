# Real-Time Streaming EHR Data Ingestion and Validation Pipeline

## Summary

This project implements a local streaming EHR ingestion and validation pipeline
using Kafka, Flink, Redis, Cassandra, and a React/Material UI dashboard. The
system generates synthetic EHR events, validates records in a streaming path,
detects duplicates, handles late and out-of-order events, quarantines invalid
records, stores accepted records, and reports throughput and latency.

## System Architecture

```text
Synthetic EHR Producer -> Kafka -> Flink -> Redis/Cassandra -> Report/Dashboard
```

Kafka provides the ingestion topic, Flink performs validation and stateful stream
processing, Redis provides patient metadata enrichment, and Cassandra stores
valid records, quarantined records, and generated alerts.

## Implemented Components

- Synthetic EHR event generator for vitals, labs, medications, and admissions
- Kafka topics for raw, valid, quarantined, and alert records
- Flink job for schema validation, clinical range checks, duplicate detection,
  event-time lateness handling, Redis enrichment, and alert generation
- Cassandra tables for accepted events, quarantined records, and alerts
- HTML report, JSON summaries, experiment matrix outputs, and React dashboard

## Experiment Summary

- Experiment runs summarized: 4
- Total synthetic events sent across runs: 30,000
- Highest producer throughput: 33,783 events/sec in `events_10000_rate_0`
- Lowest p95 latency: 783 ms in `events_5000_rate_1000`

| run | events_sent | patients | rate_limit | producer_events_per_sec | valid | quarantine | accepted_percent | avg_latency_ms | p95_latency_ms | top_quarantine_reason |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| events_10000_rate_0 | 10000 | 5000 | 0.0 | 33783.37 | 9425 | 575 | 94.25 | 5459.28 | 9826.0 | duplicate_event_id (175) |
| events_10000_rate_1000 | 10000 | 5000 | 1000.0 | 999.7 | 9425 | 575 | 94.25 | 706.32 | 959.0 | duplicate_event_id (175) |
| events_5000_rate_0 | 5000 | 5000 | 0.0 | 31811.54 | 4725 | 275 | 94.5 | 3004.82 | 5219.0 | duplicate_event_id (86) |
| events_5000_rate_1000 | 5000 | 5000 | 1000.0 | 999.53 | 4725 | 275 | 94.5 | 601.19 | 783.0 | duplicate_event_id (86) |

## Latest Pipeline Run

- Events sent: 10,000
- Synthetic patients: 5,000
- Valid records: 9,425
- Quarantined records: 575
- Accepted records: 94.25%
- Alerts: 1
- Producer throughput: 33,783 events/sec
- Average latency: 5,459 ms
- P95 latency: 9,826 ms

### Quarantine Reasons

- duplicate_event_id: 175
- contains_direct_phi: 84
- missing_patient_id: 83
- unknown_event_type: 80
- bad_event_time: 74
- vital_out_of_range: 21
- lab_out_of_range: 21
- missing_medication: 19
- missing_department: 18


## Interpretation

The results show that the local distributed pipeline can process synthetic EHR
events end-to-end and separate accepted records from invalid or duplicate
records. Quarantined records are expected because the generator intentionally
injects malformed, duplicate, late, and out-of-range events to test the
validation path. Throughput is measured at the producer side, while latency is
measured from event emission time to Cassandra storage after Flink processing.

## Limitations

This project uses synthetic data and a single-node local Docker deployment.
The results should be interpreted as local systems measurements rather than
clinical or production-scale healthcare results. A production deployment would
require stronger security, schema governance, monitoring, fault-tolerance tests,
and realistic FHIR/HL7 integration.

## Future Work

- Add Synthea-generated FHIR records for more realistic synthetic healthcare data
- Compare additional Flink parallelism and Kafka partition settings
- Add failure-recovery experiments by restarting services during ingestion
- Add alert precision analysis with controlled clinical scenarios
- Export final dashboard screenshots and experiment tables for presentation
