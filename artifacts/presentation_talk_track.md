# Presentation Talk Track

## Slide 1 - Title

- This project implements a real-time streaming EHR ingestion and validation pipeline.
- It is inspired by the paper by Andrew Psaltis and Olivia Stone.
- The goal was to build a local version that still uses real streaming-system components.

## Slide 2 - Problem and Motivation

- Healthcare data increasingly arrives continuously rather than only in batch form.
- If invalid or duplicated records enter downstream systems, analytics and alerts can be wrong.
- A streaming pipeline needs validation, duplicate handling, late-event handling, and quarantine routing.

## Slide 3 - System Architecture

- Synthetic EHR events are produced first.
- Kafka serves as the ingestion layer.
- Flink validates, deduplicates, enriches, and routes the stream.
- Redis stores patient metadata for enrichment.
- Cassandra stores accepted records, quarantined records, and alerts.
- The dashboard and reports are generated from the resulting summaries.

## Slide 4 - Implemented Components

- I implemented both the data generator and the distributed stack.
- The project includes synthetic vitals, labs, medications, and admissions.
- The pipeline writes accepted and rejected outputs separately.

## Slide 5 - Validation and Stream Logic

- The system checks required fields, timestamps, event types, and clinical value ranges.
- Duplicate detection is handled with keyed state in Flink.
- Late and out-of-order records are handled using event time and watermarks.
- Quarantine is important because bad data is not silently dropped.

## Slide 6 - Experiment Setup

- I ran four workloads: 5,000 and 10,000 events, each at 1,000 events/sec and unlimited producer rate.
- I used 5,000 synthetic patients and a 300-second lateness window.
- I intentionally injected invalid, duplicate, late, and out-of-order events.

## Slide 7 - Results Highlights

- Unlimited-rate runs achieved the highest throughput.
- Rate-limited runs achieved much lower p95 latency.
- Acceptance stayed around 94.25% to 94.5%.
- The most common quarantine reason was duplicate event ID.

## Slide 8 - Experiment Matrix Summary

- This table compares all four runs directly.
- The main tradeoff is throughput versus latency.
- When the producer is unlimited, throughput is high but latency rises significantly.

## Slide 9 - React Dashboard

- This is the live React and Material UI dashboard for the project.
- It shows accepted records, quarantined records, alerts, throughput, latency, and event distribution.
- The dashboard reads from the generated pipeline summary, so it reflects the latest run.

## Slide 10 - Runtime and Generated Output

- The Flink UI confirms that the streaming job is actually running.
- The generated report provides a stable backup view for the final results.
- Together, they show both the runtime system and the final summarized output.

## Slide 11 - Discussion and Limitations

- Redis is used for fast metadata enrichment.
- Cassandra is used as a write-heavy serving store.
- This is still a local, single-machine deployment using synthetic data only.
- A production system would need stronger fault tolerance, governance, and realistic healthcare integration.

## Slide 12 - Demo Plan

- If time allows, I can open the dashboard and Flink UI live.
- If Docker is not available, I can still present the saved experiment summary and report outputs.

## Slide 13 - Closing

- The main takeaway is that the paper-inspired streaming architecture can be implemented locally with real components.
- The project demonstrates validation, quarantine, event-time handling, and measurable throughput/latency behavior.
