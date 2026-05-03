#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if docker info >/dev/null 2>&1; then
  echo "Docker Desktop detected. Starting live presentation services..."
  docker compose up --build -d dashboard kafka redis cassandra
  docker compose run --rm kafka-init
  docker compose run --rm cassandra-init
  docker compose build flink-jobmanager producer
  docker compose up --build -d flink-jobmanager flink-taskmanager
  docker compose run --rm flink-submit
  echo "Live demo services started."
  echo "Dashboard: http://localhost:5173"
  echo "Flink:     http://localhost:8081"
  open http://localhost:5173 || true
  open http://localhost:8081 || true
else
  echo "Docker Desktop is not running. Opening saved final presentation artifacts instead."
  open "$ROOT/artifacts/experiments/experiment_summary.html" || true
  open "$ROOT/artifacts/distributed_report.html" || true
fi
