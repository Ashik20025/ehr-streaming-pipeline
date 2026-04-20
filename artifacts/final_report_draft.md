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

No experiment matrix has been generated yet. Run `scripts/run_experiment_matrix.sh` and regenerate this report.



## Latest Pipeline Run

- Events sent: 10,000
- Synthetic patients: 5,000
- Valid records: 9,425
- Quarantined records: 575
- Accepted records: 94.25%
- Alerts: 1
- Producer throughput: 28,083 events/sec
- Average latency: 5,809 ms
- P95 latency: 10,212 ms

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
