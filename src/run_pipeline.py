from __future__ import annotations

import argparse
from pathlib import Path

from ehr_pipeline.generator import synthetic_events
from ehr_pipeline.pipeline import EHRIngestionPipeline
from ehr_pipeline.report import write_report
from ehr_pipeline.storage import Storage


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    args = parse_args()
    db_path = ROOT / "data" / "ehr_pipeline.db"
    quarantine_path = ROOT / "data" / "quarantine.jsonl"
    report_path = ROOT / "artifacts" / "report.html"

    storage = Storage(db_path=db_path, quarantine_path=quarantine_path, reset=True)
    try:
        pipeline = EHRIngestionPipeline(storage, allowed_lateness_sec=args.allowed_lateness_sec)
        events = synthetic_events(
            count=args.events,
            patient_count=args.patients,
            invalid_rate=args.invalid_rate,
            duplicate_rate=args.duplicate_rate,
            out_of_order_rate=args.out_of_order_rate,
            late_rate=args.late_rate,
            allowed_lateness_sec=args.allowed_lateness_sec,
            seed=args.seed,
        )
        metrics = pipeline.run(events)
        summary = storage.fetch_summary()
        write_report(summary, report_path)
    finally:
        storage.close()

    print("Streaming EHR pipeline run complete")
    print(f"events: {metrics['total']:,}")
    print(f"accepted: {metrics['accepted']:,}")
    print(f"quarantined: {metrics['quarantined']:,}")
    print(f"duplicates: {metrics['duplicates']:,}")
    print(f"late: {metrics['late']:,}")
    print(f"out_of_order accepted: {metrics['out_of_order']:,}")
    print(f"alerts: {metrics['alerts']:,}")
    print(f"throughput: {metrics['throughput_eps']:,.0f} events/sec")
    print(f"p95 latency: {metrics['p95_latency_ms']:.3f} ms")
    print(f"sqlite: {db_path}")
    print(f"quarantine: {quarantine_path}")
    print(f"report: {report_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run local streaming EHR ingestion pipeline.")
    parser.add_argument("--events", type=int, default=20000)
    parser.add_argument("--patients", type=int, default=2000)
    parser.add_argument("--invalid-rate", type=float, default=0.05)
    parser.add_argument("--duplicate-rate", type=float, default=0.02)
    parser.add_argument("--out-of-order-rate", type=float, default=0.12)
    parser.add_argument("--late-rate", type=float, default=0.03)
    parser.add_argument("--allowed-lateness-sec", type=int, default=30)
    parser.add_argument("--seed", type=int, default=247)
    return parser.parse_args()


if __name__ == "__main__":
    main()
