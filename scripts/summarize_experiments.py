from __future__ import annotations

import argparse
import csv
import html
import json
from pathlib import Path
from typing import Any


def main() -> None:
    args = parse_args()
    experiment_dir = Path(args.experiment_dir)
    rows = [row_from_summary(path) for path in sorted(experiment_dir.glob("*/summary.json"))]
    rows = [row for row in rows if row]
    if not rows:
        raise SystemExit(f"No experiment summaries found under {experiment_dir}")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    write_csv(output_dir / "experiment_summary.csv", rows)
    write_markdown(output_dir / "experiment_summary.md", rows)
    write_html(output_dir / "experiment_summary.html", rows)
    print(f"wrote {output_dir / 'experiment_summary.csv'}")
    print(f"wrote {output_dir / 'experiment_summary.md'}")
    print(f"wrote {output_dir / 'experiment_summary.html'}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aggregate distributed EHR pipeline experiment runs.")
    parser.add_argument("--experiment-dir", default="artifacts/experiments")
    parser.add_argument("--output-dir", default="artifacts/experiments")
    return parser.parse_args()


def row_from_summary(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    totals = data["totals"]
    latency = data["latency_ms"]
    producer = data["producer"]
    return {
        "run": path.parent.name,
        "events_sent": int(producer.get("sent", 0)),
        "patients": int(producer.get("patients", 0)),
        "rate_limit": float(producer.get("rate_limit_events_per_sec", 0.0)),
        "producer_events_per_sec": round(float(producer.get("throughput_events_per_sec", 0.0)), 2),
        "valid": int(totals["valid"]),
        "quarantine": int(totals["quarantine"]),
        "alerts": int(totals["alerts"]),
        "accepted_percent": float(totals["accepted_percent"]),
        "avg_latency_ms": round(float(latency["avg_ms"]), 2),
        "p50_latency_ms": round(float(latency["p50_ms"]), 2),
        "p95_latency_ms": round(float(latency["p95_ms"]), 2),
        "max_latency_ms": round(float(latency["max_ms"]), 2),
        "top_quarantine_reason": top_reason(data.get("quarantine_reasons", {})),
    }


def top_reason(reasons: dict[str, int]) -> str:
    if not reasons:
        return "none"
    reason, count = max(reasons.items(), key=lambda item: item[1])
    return f"{reason} ({count})"


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_markdown(path: Path, rows: list[dict[str, Any]]) -> None:
    headers = list(rows[0].keys())
    lines = [
        "# Experiment Summary",
        "",
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(str(row[key]) for key in headers) + " |")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_html(path: Path, rows: list[dict[str, Any]]) -> None:
    headers = list(rows[0].keys())
    table_headers = "".join(f"<th>{html.escape(header)}</th>" for header in headers)
    table_rows = "\n".join(
        "<tr>" + "".join(f"<td>{html.escape(str(row[key]))}</td>" for key in headers) + "</tr>"
        for row in rows
    )
    body = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>EHR Pipeline Experiment Summary</title>
  <style>
    body {{ margin: 0; background: #f7faf8; color: #17211f; font-family: Arial, Helvetica, sans-serif; }}
    main {{ max-width: 1180px; margin: 0 auto; padding: 32px 18px 48px; }}
    h1 {{ margin: 0 0 8px; }}
    p {{ max-width: 860px; line-height: 1.5; }}
    table {{ width: 100%; border-collapse: collapse; background: white; border: 1px solid #d8e3df; }}
    th, td {{ border-bottom: 1px solid #d8e3df; padding: 10px; text-align: left; font-size: 0.92rem; }}
    th {{ background: #edf5f2; }}
  </style>
</head>
<body>
<main>
  <h1>EHR Pipeline Experiment Summary</h1>
  <p>
    This table aggregates repeated Kafka-Flink-Redis-Cassandra EHR streaming
    runs. Use it for the final report after collecting multiple workloads.
  </p>
  <table>
    <thead><tr>{table_headers}</tr></thead>
    <tbody>{table_rows}</tbody>
  </table>
</main>
</body>
</html>
"""
    path.write_text(body, encoding="utf-8")


if __name__ == "__main__":
    main()
