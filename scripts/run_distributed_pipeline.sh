#!/usr/bin/env bash
set -euo pipefail

EVENTS="${EVENTS:-100000}"
PATIENTS="${PATIENTS:-10000}"
RATE="${RATE:-0}"
ALLOWED_LATENESS="${ALLOWED_LATENESS:-300}"
RESET="${RESET:-1}"

if [[ "$RESET" == "1" ]]; then
  docker compose down -v --remove-orphans
fi

docker compose up --build -d kafka redis cassandra
docker compose run --rm kafka-init
docker compose run --rm cassandra-init
docker compose build flink-jobmanager producer
docker compose up --build -d flink-jobmanager flink-taskmanager
docker compose run --rm flink-submit
docker compose run --rm producer \
  --events "$EVENTS" \
  --patients "$PATIENTS" \
  --rate "$RATE" \
  --allowed-lateness-sec "$ALLOWED_LATENESS" \
  --metrics-output /artifacts/distributed_run_metrics.json

echo "Waiting for Flink to drain Kafka into Cassandra..."
sleep 20
docker compose run --rm reporter

echo "Report written to artifacts/distributed_report.html"
