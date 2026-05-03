#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

open "$ROOT/artifacts/EECS247_Final_Presentation_Hanuman_Akshintala.pptx" || true
open "$ROOT/artifacts/experiments/experiment_summary.html" || true
open "$ROOT/artifacts/distributed_report.html" || true
open "$ROOT/artifacts/final_report_draft.md" || true
