from __future__ import annotations

import html
import json
from pathlib import Path
from typing import Any


def write_report(summary: dict[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    metrics = summary["metrics"]
    reasons = summary["reasons"]
    event_types = summary["event_types"]
    throughput = metrics.get("throughput_buckets", [])

    body = f"""
    <main>
      <section class="intro">
        <p class="eyebrow">Local streaming EHR pipeline run</p>
        <h1>Real-Time EHR Ingestion and Validation Report</h1>
        <p>
          Synthetic EHR events were streamed through validation, deduplication,
          event-time lateness handling, quarantine, SQLite serving storage, and a
          possible-sepsis CEP rule.
        </p>
      </section>

      <section class="metrics">
        {_metric("Total events", metrics.get("total", 0))}
        {_metric("Accepted", metrics.get("accepted", 0))}
        {_metric("Quarantined", metrics.get("quarantined", 0))}
        {_metric("Throughput", f"{metrics.get('throughput_eps', 0):,.0f} evt/sec")}
        {_metric("P95 latency", f"{metrics.get('p95_latency_ms', 0):.3f} ms")}
        {_metric("CEP alerts", metrics.get("alerts", 0))}
      </section>

      <section class="grid">
        <article>
          <h2>Throughput by Chunk</h2>
          {_bar_chart(throughput, "evt/sec")}
        </article>
        <article>
          <h2>Valid vs Quarantined</h2>
          {_bar_chart([metrics.get("accepted", 0), metrics.get("quarantined", 0)], "records", ["valid", "quarantine"])}
        </article>
        <article>
          <h2>Event Types Stored</h2>
          {_bar_chart(list(event_types.values()), "records", list(event_types.keys()))}
        </article>
        <article>
          <h2>Quarantine Reasons</h2>
          {_bar_chart(list(reasons.values()), "records", list(reasons.keys()))}
        </article>
      </section>

      <section>
        <h2>Sample Quarantined Records</h2>
        <table>
          <thead><tr><th>Reason</th><th>Record Preview</th></tr></thead>
          <tbody>
            {''.join(_sample_row(reason, sample) for reason, sample in summary["samples"])}
          </tbody>
        </table>
      </section>

      <section>
        <h2>Run Metrics JSON</h2>
        <pre>{html.escape(json.dumps(metrics, indent=2, sort_keys=True))}</pre>
      </section>
    </main>
    """
    output_path.write_text(_page(body), encoding="utf-8")


def _page(body: str) -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Streaming EHR Pipeline Report</title>
  <style>
    :root {{
      color: #17211f;
      background: #f7faf8;
      font-family: Arial, Helvetica, sans-serif;
    }}
    body {{ margin: 0; background: #f7faf8; }}
    main {{ max-width: 1160px; margin: 0 auto; padding: 32px 18px 48px; }}
    .intro {{ margin-bottom: 24px; }}
    .eyebrow {{ color: #006b5f; font-weight: 700; text-transform: uppercase; font-size: 0.78rem; }}
    h1 {{ margin: 0 0 10px; font-size: 2rem; }}
    h2 {{ margin: 0 0 14px; font-size: 1.1rem; }}
    p {{ max-width: 780px; line-height: 1.5; }}
    .metrics {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; margin: 22px 0; }}
    .metric, article {{ background: white; border: 1px solid #d8e3df; border-radius: 6px; padding: 16px; }}
    .metric strong {{ display: block; font-size: 1.45rem; margin-top: 5px; }}
    .metric span {{ color: #50625c; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(310px, 1fr)); gap: 14px; }}
    svg {{ width: 100%; height: auto; display: block; }}
    table {{ width: 100%; border-collapse: collapse; background: white; border: 1px solid #d8e3df; }}
    th, td {{ text-align: left; border-bottom: 1px solid #e5ece9; padding: 10px; vertical-align: top; }}
    th {{ background: #edf5f2; }}
    pre {{ overflow: auto; background: #10201c; color: #e7fff8; padding: 14px; border-radius: 6px; }}
  </style>
</head>
<body>
{body}
</body>
</html>
"""


def _metric(label: str, value: object) -> str:
    return f'<div class="metric"><span>{html.escape(label)}</span><strong>{html.escape(str(value))}</strong></div>'


def _bar_chart(values: list[float | int], unit: str, labels: list[str] | None = None) -> str:
    if not values:
        return "<p>No data recorded.</p>"
    labels = labels or [str(i + 1) for i in range(len(values))]
    width = 620
    height = 250
    left = 56
    bottom = 36
    top = 18
    max_value = max(max(values), 1)
    usable_h = height - top - bottom
    bar_w = max(8, (width - left - 18) / len(values) - 6)
    bars = []
    for i, value in enumerate(values):
        x = left + i * (bar_w + 6)
        bar_h = (float(value) / max_value) * usable_h
        y = top + usable_h - bar_h
        label = html.escape(labels[i][:18])
        bars.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" height="{bar_h:.1f}" fill="#00856f"></rect>')
        bars.append(f'<text x="{x + bar_w / 2:.1f}" y="{height - 12}" text-anchor="middle" font-size="10">{label}</text>')
    return f"""
    <svg viewBox="0 0 {width} {height}" role="img" aria-label="Bar chart in {html.escape(unit)}">
      <line x1="{left}" y1="{top}" x2="{left}" y2="{height - bottom}" stroke="#879892"></line>
      <line x1="{left}" y1="{height - bottom}" x2="{width - 8}" y2="{height - bottom}" stroke="#879892"></line>
      <text x="8" y="22" font-size="11">max {max_value:,.1f} {html.escape(unit)}</text>
      {''.join(bars)}
    </svg>
    """


def _sample_row(reason: str, sample: str) -> str:
    return (
        "<tr>"
        f"<td>{html.escape(reason)}</td>"
        f"<td><code>{html.escape(sample)}</code></td>"
        "</tr>"
    )
