from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def main() -> None:
    args = parse_args()
    experiment_dir = Path(args.experiment_dir)
    output = Path(args.output)
    rows = load_rows(experiment_dir)
    latest = load_latest(Path(args.latest_summary))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(render_report(rows, latest), encoding="utf-8")
    print(f"wrote {output}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a Markdown final report draft from pipeline results.")
    parser.add_argument("--experiment-dir", default="artifacts/experiments")
    parser.add_argument("--latest-summary", default="artifacts/distributed_summary.json")
    parser.add_argument("--output", default="artifacts/final_report_draft.md")
    return parser.parse_args()


def load_rows(experiment_dir: Path) -> list[dict[str, Any]]:
    summary_path = experiment_dir / "experiment_summary.json"
    if summary_path.exists():
        return json.loads(summary_path.read_text(encoding="utf-8"))

    rows = []
    for path in sorted(experiment_dir.glob("*/summary.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        totals = data["totals"]
        latency = data["latency_ms"]
        producer = data["producer"]
        rows.append(
            {
                "run": path.parent.name,
                "events_sent": int(producer.get("sent", 0)),
                "patients": int(producer.get("patients", 0)),
                "rate_limit": float(producer.get("rate_limit_events_per_sec", 0.0)),
                "producer_events_per_sec": round(float(producer.get("throughput_events_per_sec", 0.0)), 2),
                "valid": int(totals["valid"]),
                "quarantine": int(totals["quarantine"]),
                "alerts": int(totals["alerts"]),
                "accepted_percent": float(totals["accepted_percent"]),
                "quarantine_percent": round(100.0 - float(totals["accepted_percent"]), 2),
                "avg_latency_ms": round(float(latency["avg_ms"]), 2),
                "p50_latency_ms": round(float(latency["p50_ms"]), 2),
                "p95_latency_ms": round(float(latency["p95_ms"]), 2),
                "max_latency_ms": round(float(latency["max_ms"]), 2),
                "top_quarantine_reason": top_reason(data.get("quarantine_reasons", {})),
                "event_mix": event_mix(data.get("event_counts", {})),
            }
        )
    return rows


def load_latest(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def render_report(rows: list[dict[str, Any]], latest: dict[str, Any]) -> str:
    best_throughput = max(rows, key=lambda row: row["producer_events_per_sec"]) if rows else None
    best_latency = min(rows, key=lambda row: row["p95_latency_ms"]) if rows else None
    total_events = sum(row["events_sent"] for row in rows)
    table = render_table(rows)
    latest_section = render_latest(latest)

    return f"""# Real-Time Streaming EHR Data Ingestion and Validation Pipeline

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

{experiment_overview(rows, best_throughput, best_latency, total_events)}

{table}

{latest_section}

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
"""


def experiment_overview(
    rows: list[dict[str, Any]],
    best_throughput: dict[str, Any] | None,
    best_latency: dict[str, Any] | None,
    total_events: int,
) -> str:
    if not rows:
        return (
            "No experiment matrix has been generated yet. Run "
            "`scripts/run_experiment_matrix.sh` and regenerate this report."
        )
    return "\n".join(
        [
            f"- Experiment runs summarized: {len(rows)}",
            f"- Total synthetic events sent across runs: {total_events:,}",
            f"- Highest producer throughput: {best_throughput['producer_events_per_sec']:,.0f} events/sec in `{best_throughput['run']}`",
            f"- Lowest p95 latency: {best_latency['p95_latency_ms']:,.0f} ms in `{best_latency['run']}`",
        ]
    )


def render_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return ""
    columns = [
        "run",
        "events_sent",
        "patients",
        "rate_limit",
        "producer_events_per_sec",
        "valid",
        "quarantine",
        "accepted_percent",
        "avg_latency_ms",
        "p95_latency_ms",
        "top_quarantine_reason",
    ]
    header = "| " + " | ".join(columns) + " |"
    divider = "| " + " | ".join("---" for _ in columns) + " |"
    body = ["| " + " | ".join(str(row[column]) for column in columns) + " |" for row in rows]
    return "\n".join([header, divider, *body])


def render_latest(latest: dict[str, Any]) -> str:
    if not latest:
        return "## Latest Pipeline Run\n\nNo latest distributed summary file was found."
    totals = latest["totals"]
    latency = latest["latency_ms"]
    producer = latest["producer"]
    reasons = latest.get("quarantine_reasons", {})
    reason_lines = "\n".join(f"- {reason}: {count}" for reason, count in sorted(reasons.items(), key=lambda item: item[1], reverse=True))
    return f"""## Latest Pipeline Run

- Events sent: {int(producer.get("sent", 0)):,}
- Synthetic patients: {int(producer.get("patients", 0)):,}
- Valid records: {int(totals.get("valid", 0)):,}
- Quarantined records: {int(totals.get("quarantine", 0)):,}
- Accepted records: {float(totals.get("accepted_percent", 0.0)):.2f}%
- Alerts: {int(totals.get("alerts", 0)):,}
- Producer throughput: {float(producer.get("throughput_events_per_sec", 0.0)):,.0f} events/sec
- Average latency: {float(latency.get("avg_ms", 0.0)):,.0f} ms
- P95 latency: {float(latency.get("p95_ms", 0.0)):,.0f} ms

### Quarantine Reasons

{reason_lines or "- none"}
"""


def top_reason(reasons: dict[str, int]) -> str:
    if not reasons:
        return "none"
    reason, count = max(reasons.items(), key=lambda item: item[1])
    return f"{reason} ({count})"


def event_mix(counts: dict[str, int]) -> str:
    if not counts:
        return "none"
    return ", ".join(f"{name}:{count}" for name, count in sorted(counts.items()))


if __name__ == "__main__":
    main()
