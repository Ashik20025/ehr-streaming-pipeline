from __future__ import annotations

import argparse
import json
import html
import statistics
import time
from pathlib import Path

from cassandra.cluster import Cluster


EVENT_TYPES = ("admission", "lab", "medication", "vital")
REASONS = (
    "late_event",
    "duplicate_event_id",
    "bad_event_time",
    "contains_direct_phi",
    "missing_patient_id",
    "unknown_event_type",
    "vital_out_of_range",
    "lab_out_of_range",
    "missing_department",
    "missing_medication",
    "bad_medication_dose",
)


def main() -> None:
    args = parse_args()
    session = wait_for_cassandra(args.cassandra_host, args.cassandra_port)
    try:
        summary = build_summary(session, Path(args.metrics_input))
        write_report(Path(args.output), summary)
        if args.json_output:
            write_json_summary(Path(args.json_output), summary)
        print(f"wrote {args.output}")
        print(
            f"valid={summary['totals']['valid']:,} "
            f"quarantine={summary['totals']['quarantine']:,} "
            f"alerts={summary['totals']['alerts']:,}"
        )
    finally:
        session.shutdown()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate an HTML report from Cassandra pipeline tables.")
    parser.add_argument("--cassandra-host", default="cassandra")
    parser.add_argument("--cassandra-port", type=int, default=9042)
    parser.add_argument("--output", default="/artifacts/distributed_report.html")
    parser.add_argument("--metrics-input", default="/artifacts/distributed_run_metrics.json")
    parser.add_argument("--json-output", default="/artifacts/distributed_summary.json")
    return parser.parse_args()


def wait_for_cassandra(host: str, port: int):
    last_exc = None
    for _ in range(60):
        try:
            cluster = Cluster([host], port=port)
            session = cluster.connect("ehr")
            session.execute("select release_version from system.local")
            return session
        except Exception as exc:
            last_exc = exc
            time.sleep(2)
    raise RuntimeError(f"Cassandra did not become ready: {last_exc}") from last_exc


def count_partition(session, table: str, key: str, value: str) -> int:
    result = session.execute(f"select count(*) from {table} where {key} = %s", (value,))
    return int(result.one()[0])


def latency_summary(session) -> dict[str, float]:
    rows = session.execute("select ingestion_latency_ms from events_by_id")
    values = sorted(float(row.ingestion_latency_ms) for row in rows if row.ingestion_latency_ms is not None)
    if not values:
        return {"avg_ms": 0.0, "p50_ms": 0.0, "p95_ms": 0.0, "max_ms": 0.0}
    p95_index = min(len(values) - 1, int(len(values) * 0.95))
    return {
        "avg_ms": statistics.fmean(values),
        "p50_ms": statistics.median(values),
        "p95_ms": values[p95_index],
        "max_ms": values[-1],
    }


def read_metrics(path: Path) -> dict[str, float]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def build_summary(session, metrics_path: Path) -> dict[str, object]:
    event_counts = {event_type: count_partition(session, "events_by_type", "event_type", event_type) for event_type in EVENT_TYPES}
    reason_counts = {reason: count_partition(session, "quarantine_by_reason", "reason", reason) for reason in REASONS}
    alert_count = count_partition(session, "alerts_by_type", "alert_type", "possible_sepsis")
    latency = latency_summary(session)
    producer_metrics = read_metrics(metrics_path)
    total_valid = sum(event_counts.values())
    total_quarantine = sum(reason_counts.values())
    total_records = total_valid + total_quarantine
    return {
        "pipeline": "Kafka-Flink-Redis-Cassandra EHR streaming pipeline",
        "generated_at_epoch_ms": int(time.time() * 1000),
        "totals": {
            "valid": total_valid,
            "quarantine": total_quarantine,
            "alerts": alert_count,
            "processed": total_records,
            "accepted_percent": round((total_valid / max(total_records, 1)) * 100, 2),
        },
        "event_counts": event_counts,
        "quarantine_reasons": {k: v for k, v in reason_counts.items() if v},
        "latency_ms": latency,
        "producer": producer_metrics,
    }


def write_json_summary(output: Path, summary: dict[str, object]) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(summary, indent=2), encoding="utf-8")


def write_report(output: Path, summary: dict[str, object]) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    totals = summary["totals"]
    event_counts = summary["event_counts"]
    reason_counts = summary["quarantine_reasons"]
    latency = summary["latency_ms"]
    producer_metrics = summary["producer"]
    total_valid = int(totals["valid"])
    total_quarantine = int(totals["quarantine"])
    throughput = float(producer_metrics.get("throughput_events_per_sec", 0.0))
    body = f"""
    <main>
      <p class="eyebrow">Kafka + Flink + Redis + Cassandra</p>
      <h1>Distributed Streaming EHR Pipeline Report</h1>
      <p>
        This report is generated from Cassandra after the local distributed pipeline
        consumes synthetic EHR events from Kafka, validates them in Flink, enriches
        them with Redis patient metadata, stores valid events, and quarantines bad
        records.
      </p>
      <section class="metrics">
        {metric("Valid records", total_valid)}
        {metric("Quarantined records", total_quarantine)}
        {metric("Possible-sepsis alerts", int(totals["alerts"]))}
        {metric("Producer throughput", f"{throughput:,.0f} events/sec")}
        {metric("Average latency", f"{latency['avg_ms']:,.0f} ms")}
        {metric("P95 latency", f"{latency['p95_ms']:,.0f} ms")}
      </section>
      <section class="grid">
        <article>
          <h2>Valid Vs Quarantined</h2>
          {bar_chart({"valid": total_valid, "quarantined": total_quarantine})}
          <p>{float(totals["accepted_percent"]):.1f}% of processed records were accepted.</p>
        </article>
        <article>
          <h2>Throughput And Latency</h2>
          {bar_chart({
              "producer_events_per_sec": int(throughput),
              "avg_latency_ms": int(latency["avg_ms"]),
              "p50_latency_ms": int(latency["p50_ms"]),
              "p95_latency_ms": int(latency["p95_ms"]),
          })}
        </article>
        <article>
          <h2>Events Stored By Type</h2>
          {bar_chart(event_counts)}
        </article>
        <article>
          <h2>Quarantine Reasons</h2>
          {bar_chart({k: v for k, v in reason_counts.items() if v})}
        </article>
      </section>
    </main>
    """
    output.write_text(page(body), encoding="utf-8")


def page(body: str) -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Distributed Streaming EHR Pipeline Report</title>
  <style>
    body {{ margin: 0; background: #f7faf8; color: #17211f; font-family: Arial, Helvetica, sans-serif; }}
    main {{ max-width: 1120px; margin: 0 auto; padding: 34px 18px 50px; }}
    .eyebrow {{ color: #006b5f; font-weight: 700; text-transform: uppercase; font-size: 0.78rem; }}
    h1 {{ margin: 0 0 10px; font-size: 2rem; }}
    p {{ max-width: 820px; line-height: 1.5; }}
    .metrics {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin: 24px 0; }}
    .metric, article {{ background: white; border: 1px solid #d8e3df; border-radius: 6px; padding: 16px; }}
    .metric span {{ color: #50625c; }}
    .metric strong {{ display: block; font-size: 1.5rem; margin-top: 5px; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(330px, 1fr)); gap: 14px; }}
    .row {{ display: grid; grid-template-columns: 180px 1fr 90px; align-items: center; gap: 10px; margin: 8px 0; }}
    .label {{ overflow-wrap: anywhere; font-size: 0.92rem; }}
    .track {{ height: 18px; background: #edf5f2; border-radius: 5px; overflow: hidden; }}
    .bar {{ height: 100%; background: #00856f; }}
    .count {{ text-align: right; font-variant-numeric: tabular-nums; }}
  </style>
</head>
<body>
{body}
</body>
</html>
"""


def metric(label: str, value: object) -> str:
    display = f"{value:,}" if isinstance(value, int) else str(value)
    return f'<div class="metric"><span>{html.escape(label)}</span><strong>{html.escape(display)}</strong></div>'


def bar_chart(values: dict[str, int]) -> str:
    if not values:
        return "<p>No rows yet.</p>"
    max_value = max(values.values()) or 1
    rows = []
    for label, value in sorted(values.items(), key=lambda item: item[1], reverse=True):
        width = (value / max_value) * 100
        rows.append(
            '<div class="row">'
            f'<div class="label">{html.escape(label)}</div>'
            f'<div class="track"><div class="bar" style="width:{width:.1f}%"></div></div>'
            f'<div class="count">{value:,}</div>'
            '</div>'
        )
    return "\n".join(rows)


if __name__ == "__main__":
    main()
