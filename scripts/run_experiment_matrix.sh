#!/usr/bin/env bash
set -euo pipefail

EVENTS_LIST="${EVENTS_LIST:-5000 10000}"
RATES_LIST="${RATES_LIST:-1000 0}"
PATIENTS="${PATIENTS:-1000}"
ALLOWED_LATENESS="${ALLOWED_LATENESS:-300}"
OUT_DIR="${OUT_DIR:-artifacts/experiments}"

mkdir -p "$OUT_DIR"

for events in $EVENTS_LIST; do
  for rate in $RATES_LIST; do
    run_id="events_${events}_rate_${rate}"
    run_dir="$OUT_DIR/$run_id"
    mkdir -p "$run_dir"
    echo "Running experiment: $run_id"
    EVENTS="$events" \
      PATIENTS="$PATIENTS" \
      RATE="$rate" \
      ALLOWED_LATENESS="$ALLOWED_LATENESS" \
      RESET=1 \
      ./scripts/run_distributed_pipeline.sh
    cp artifacts/distributed_report.html "$run_dir/report.html"
    cp artifacts/distributed_run_metrics.json "$run_dir/producer_metrics.json"
    cp artifacts/distributed_summary.json "$run_dir/summary.json"
  done
done

python3 scripts/summarize_experiments.py --experiment-dir "$OUT_DIR" --output-dir "$OUT_DIR"
python3 scripts/generate_final_report.py --experiment-dir "$OUT_DIR" --output artifacts/final_report_draft.md
echo "Experiment matrix complete: $OUT_DIR/experiment_summary.html"
echo "Final report draft: artifacts/final_report_draft.md"
