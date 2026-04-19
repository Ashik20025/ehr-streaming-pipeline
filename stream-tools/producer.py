from __future__ import annotations

import argparse
import copy
import json
import random
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterator

import redis
from kafka import KafkaProducer


EVENT_TYPES = ("vital", "lab", "medication", "admission")
VITALS = (
    ("heart_rate", "bpm", 55, 150),
    ("systolic_bp", "mmHg", 88, 185),
    ("diastolic_bp", "mmHg", 48, 110),
    ("temperature_c", "C", 36.0, 40.7),
    ("spo2", "%", 88, 100),
)
LABS = (
    ("wbc", "10^9/L", 3.5, 18.0),
    ("lactate", "mmol/L", 0.6, 4.2),
    ("creatinine", "mg/dL", 0.4, 2.8),
)
MEDICATIONS = ("vancomycin", "ceftriaxone", "norepinephrine", "acetaminophen")
DEPARTMENTS = ("ED", "ICU", "medicine", "surgery", "cardiology")


def main() -> None:
    args = parse_args()
    rng = random.Random(args.seed)
    wait_for_redis(args.redis_host, args.redis_port)
    seed_redis(args.redis_host, args.redis_port, args.patients, rng)
    producer = wait_for_kafka(args.bootstrap_servers)

    start = time.perf_counter()
    sent = 0
    next_send = start
    for event in synthetic_events(args, rng):
        producer.send(args.topic, event)
        sent += 1
        if args.rate > 0:
            next_send += 1.0 / args.rate
            delay = next_send - time.perf_counter()
            if delay > 0:
                time.sleep(delay)
        if sent % args.progress_interval == 0:
            elapsed = max(time.perf_counter() - start, 1e-9)
            print(f"sent={sent:,} rate={sent / elapsed:,.0f} events/sec")

    producer.flush()
    elapsed = max(time.perf_counter() - start, 1e-9)
    metrics = {
        "sent": sent,
        "elapsed_seconds": elapsed,
        "throughput_events_per_sec": sent / elapsed,
        "patients": args.patients,
        "rate_limit_events_per_sec": args.rate,
        "allowed_lateness_sec": args.allowed_lateness_sec,
        "invalid_rate": args.invalid_rate,
        "duplicate_rate": args.duplicate_rate,
        "out_of_order_rate": args.out_of_order_rate,
        "late_rate": args.late_rate,
        "seed": args.seed,
    }
    if args.metrics_output:
        metrics_path = Path(args.metrics_output)
        metrics_path.parent.mkdir(parents=True, exist_ok=True)
        metrics_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    print(f"producer complete: sent={sent:,} elapsed={elapsed:.2f}s throughput={sent / elapsed:,.0f} events/sec")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream synthetic EHR events into Kafka.")
    parser.add_argument("--bootstrap-servers", default="kafka:9092")
    parser.add_argument("--topic", default="ehr.raw")
    parser.add_argument("--redis-host", default="redis")
    parser.add_argument("--redis-port", type=int, default=6379)
    parser.add_argument("--events", type=int, default=100_000)
    parser.add_argument("--patients", type=int, default=10_000)
    parser.add_argument("--rate", type=float, default=0.0, help="Events/sec. 0 means send as fast as possible.")
    parser.add_argument("--invalid-rate", type=float, default=0.04)
    parser.add_argument("--duplicate-rate", type=float, default=0.02)
    parser.add_argument("--out-of-order-rate", type=float, default=0.12)
    parser.add_argument("--late-rate", type=float, default=0.03)
    parser.add_argument("--allowed-lateness-sec", type=int, default=30)
    parser.add_argument("--seed", type=int, default=247)
    parser.add_argument("--progress-interval", type=int, default=10_000)
    parser.add_argument("--metrics-output", default="", help="Optional JSON file for producer throughput metrics.")
    return parser.parse_args()


def wait_for_kafka(bootstrap_servers: str) -> KafkaProducer:
    for attempt in range(1, 61):
        try:
            return KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
                linger_ms=5,
                batch_size=64 * 1024,
                compression_type="gzip",
                acks="all",
                retries=5,
            )
        except Exception as exc:
            if attempt == 60:
                raise
            print(f"waiting for Kafka ({attempt}/60): {exc}")
            time.sleep(2)
    raise RuntimeError("Kafka did not become ready")


def wait_for_redis(host: str, port: int) -> None:
    client = redis.Redis(host=host, port=port, decode_responses=True)
    for attempt in range(1, 61):
        try:
            client.ping()
            return
        except Exception as exc:
            if attempt == 60:
                raise
            print(f"waiting for Redis ({attempt}/60): {exc}")
            time.sleep(1)


def seed_redis(host: str, port: int, patient_count: int, rng: random.Random) -> None:
    client = redis.Redis(host=host, port=port, decode_responses=True)
    pipe = client.pipeline(transaction=False)
    for index in range(patient_count):
        patient_id = f"P{index:07d}"
        pipe.hset(
            f"patient:{patient_id}",
            mapping={
                "age_band": rng.choice(("18-34", "35-49", "50-64", "65-79", "80+")),
                "home_department": rng.choice(DEPARTMENTS),
                "risk_score": f"{rng.random():.3f}",
            },
        )
        if index and index % 5000 == 0:
            pipe.execute()
    pipe.execute()
    print(f"seeded Redis metadata for {patient_count:,} patients")


def synthetic_events(args: argparse.Namespace, rng: random.Random) -> Iterator[dict[str, Any]]:
    base_time = datetime.now(UTC).replace(microsecond=0)
    previous: list[dict[str, Any]] = []

    for index in range(args.events):
        if previous and rng.random() < args.duplicate_rate:
            duplicate = copy.deepcopy(rng.choice(previous[-1000:]))
            duplicate["source_emit_time"] = iso_z(datetime.now(UTC))
            duplicate["injected_issue"] = "duplicate"
            yield duplicate
            continue

        event_time = base_time + timedelta(milliseconds=index * 10)
        injected_issue = "none"
        if rng.random() < args.late_rate:
            event_time -= timedelta(seconds=args.allowed_lateness_sec + rng.randint(30, 240))
            injected_issue = "late"
        elif rng.random() < args.out_of_order_rate:
            event_time -= timedelta(seconds=rng.randint(1, args.allowed_lateness_sec))
            injected_issue = "out_of_order"

        event_type = rng.choice(EVENT_TYPES)
        event = {
            "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
            "patient_id": f"P{rng.randrange(args.patients):07d}",
            "event_type": event_type,
            "event_time": iso_z(event_time),
            "source_emit_time": iso_z(datetime.now(UTC)),
            "source": rng.choice(("bedside-monitor", "lab-system", "ehr-hl7", "patient-portal")),
            "schema_version": "ehr.v1",
            "payload": payload_for(event_type, rng),
            "injected_issue": injected_issue,
        }
        if rng.random() < args.invalid_rate:
            corrupt(event, rng)
            event["injected_issue"] = "invalid"
        previous.append(copy.deepcopy(event))
        yield event


def payload_for(event_type: str, rng: random.Random) -> dict[str, Any]:
    if event_type == "vital":
        name, unit, low, high = rng.choice(VITALS)
        return {"name": name, "value": round(rng.uniform(low, high), 1), "unit": unit}
    if event_type == "lab":
        test, unit, low, high = rng.choice(LABS)
        return {"test": test, "result": round(rng.uniform(low, high), 2), "unit": unit}
    if event_type == "medication":
        return {
            "medication": rng.choice(MEDICATIONS),
            "dose": round(rng.uniform(1, 500), 1),
            "unit": "mg",
            "route": rng.choice(("IV", "PO", "IM")),
        }
    return {"department": rng.choice(DEPARTMENTS), "acuity": rng.randint(1, 5)}


def corrupt(event: dict[str, Any], rng: random.Random) -> None:
    issue = rng.choice(("drop_patient", "bad_type", "bad_time", "bad_range", "phi"))
    if issue == "drop_patient":
        event.pop("patient_id", None)
    elif issue == "bad_type":
        event["event_type"] = "fax"
    elif issue == "bad_time":
        event["event_time"] = "not-a-timestamp"
    elif issue == "bad_range":
        if event.get("event_type") == "vital":
            event["payload"] = {"name": "heart_rate", "value": 999, "unit": "bpm"}
        elif event.get("event_type") == "lab":
            event["payload"] = {"test": "lactate", "result": -2, "unit": "mmol/L"}
        else:
            event["payload"] = {"dose": -10}
    else:
        event["patient_name"] = "Jane Example"


def iso_z(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


if __name__ == "__main__":
    main()
